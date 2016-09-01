package com.intropro.prairie.unit.sshd;

import com.intropro.prairie.unit.common.BaseUnit;
import com.intropro.prairie.unit.common.PortProvider;
import com.intropro.prairie.unit.common.annotation.PrairieUnit;
import com.intropro.prairie.unit.common.exception.DestroyUnitException;
import com.intropro.prairie.unit.common.exception.InitUnitException;
import com.intropro.prairie.unit.kerberos.KerberosUnit;
import com.intropro.prairie.unit.kerberos.KerberosUser;
import com.intropro.prairie.unit.kerberos.exception.KerberosException;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.sshd.common.NamedFactory;
import org.apache.sshd.common.keyprovider.MappedKeyPairProvider;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.auth.UserAuth;
import org.apache.sshd.server.auth.gss.GSSAuthenticator;
import org.apache.sshd.server.auth.gss.UserAuthGSSFactory;
import org.apache.sshd.server.auth.password.UserAuthPasswordFactory;
import org.apache.sshd.server.auth.pubkey.KeySetPublickeyAuthenticator;
import org.apache.sshd.server.auth.pubkey.UserAuthPublicKeyFactory;
import org.apache.sshd.server.scp.ScpCommandFactory;
import org.apache.sshd.server.shell.ProcessShellFactory;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemWriter;

import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.*;

/**
 * Created by presidentio on 8/25/16.
 */
public class SshdUnit extends BaseUnit {

    private static final Logger LOGGER = LogManager.getLogger(SshdUnit.class);

    @PrairieUnit
    private KerberosUnit kerberosUnit;

    private SshServer sshServer;
    private CollectionPasswordAuthenticator collectionPasswordAuthenticator = new CollectionPasswordAuthenticator();
    private String defaultUsername = System.getProperty("user.name");
    private String defaultPassword = "prairie";
    private String host = "localhost";
    private Path privateKeyPath;
    private Path publicKeyPath;
    private PublicKey publicKey;
    private PrivateKey privateKey;

    public SshdUnit() {
        super("sshd");
    }

    @Override
    protected void init() throws InitUnitException {
        try {
            host = InetAddress.getLocalHost().getCanonicalHostName().toLowerCase();
        } catch (UnknownHostException e) {
            LOGGER.warn("Failed to get canonical host name. Using default: " + host);
        }
        sshServer = SshServer.setUpDefaultServer();
        sshServer.setPort(PortProvider.nextPort());
        sshServer.setHost(host);
        try {
            sshServer.setKeyPairProvider(new MappedKeyPairProvider(readHostKeyPair()));
        } catch (NoSuchAlgorithmException | InvalidKeySpecException | IOException  e) {
            LOGGER.error("Failed to read host key pair", e);
        }
        sshServer.setShellFactory(new ProcessShellFactory("/bin/sh", "-i", "-l"));
        sshServer.setCommandFactory(new ScpCommandFactory.Builder().build());
        setupSecurity(sshServer);
        try {
            LOGGER.info("starting ssh server on " + sshServer.getPort());
            sshServer.start();
        } catch (IOException e) {
            throw new InitUnitException("Failed to start ssh server on port " + sshServer.getPort(), e);
        }
    }

    private KeyPair readHostKeyPair() throws IOException, InvalidKeySpecException, NoSuchAlgorithmException {
        KeyFactory kf = KeyFactory.getInstance("RSA");

        byte[] privateKeyBytes = IOUtils.toByteArray(SshdUnit.class.getResourceAsStream("/keys/private.der"));
        PKCS8EncodedKeySpec privateSpec = new PKCS8EncodedKeySpec(privateKeyBytes);
        PrivateKey privateKey = kf.generatePrivate(privateSpec);

        byte[] publicKeyBytes = IOUtils.toByteArray(SshdUnit.class.getResourceAsStream("/keys/public.der"));
        X509EncodedKeySpec publicSpec =
                new X509EncodedKeySpec(publicKeyBytes);
        PublicKey publicKey = kf.generatePublic(publicSpec);
        return new KeyPair(publicKey, privateKey);
    }

    private void setupSecurity(SshServer sshServer) {
        //key authentication
        try {
            generateKeys();
        } catch (IOException | NoSuchAlgorithmException | NoSuchProviderException e) {
            LOGGER.error("Failed to save keys", e);
        }
        sshServer.setPublickeyAuthenticator(new KeySetPublickeyAuthenticator(Collections.singletonList(publicKey)));

        //password authentication
        collectionPasswordAuthenticator.addUser(defaultUsername, defaultPassword);
        sshServer.setPasswordAuthenticator(collectionPasswordAuthenticator);

        //kerberos authentication
        List<NamedFactory<UserAuth>> userAuthFactories = new ArrayList<>(3);
        userAuthFactories.add(new UserAuthGSSFactory());
        userAuthFactories.add(new UserAuthPublicKeyFactory());
        userAuthFactories.add(new UserAuthPasswordFactory());
        sshServer.setUserAuthFactories(userAuthFactories);

        try {
            String username = "host/" + host;
            KerberosUser kerberosUser = kerberosUnit.getKerberosUserManager().addUser(username, "pass");
            GSSAuthenticator authenticator = new GSSAuthenticator();
            authenticator.setKeytabFile(kerberosUser.getKeytab().getAbsolutePath());
            authenticator.setServicePrincipalName(kerberosUser.getPrincipal());
            sshServer.setGSSAuthenticator(authenticator);
        } catch (KerberosException e) {
            LOGGER.error("Kerberos authentication does not work", e);
        }
    }

    private void generateKeys() throws IOException, NoSuchAlgorithmException, NoSuchProviderException {
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(1024, new SecureRandom(new byte[1024]));
        KeyPair keyPair = keyPairGenerator.generateKeyPair();
        privateKey = keyPair.getPrivate();
        publicKey = keyPair.getPublic();

        Path keysDir = getTmpDir().resolve("keys");
        keysDir.toFile().mkdirs();

        // Store Private Key.
        this.privateKeyPath = keysDir.resolve("private.key");
        PemWriter pemWriter = new PemWriter(new FileWriter(this.privateKeyPath.toFile()));
        pemWriter.writeObject(new PemObject("RSA PRIVATE KEY", privateKey.getEncoded()));
        pemWriter.close();

        // Store Public Key.
        this.publicKeyPath = keysDir.resolve("public.key");
        pemWriter = new PemWriter(new FileWriter(this.publicKeyPath.toFile()));
        pemWriter.writeObject(new PemObject("RSA PUBLIC KEY", publicKey.getEncoded()));
        pemWriter.close();

        Set<PosixFilePermission> perms = new HashSet<>();
        perms.add(PosixFilePermission.OWNER_READ);
        perms.add(PosixFilePermission.OWNER_WRITE);
        Files.setPosixFilePermissions(this.privateKeyPath, perms);
        Files.setPosixFilePermissions(this.publicKeyPath, perms);
    }

    @Override
    protected void destroy() throws DestroyUnitException {
        try {
            sshServer.stop();
        } catch (IOException e) {
            throw new DestroyUnitException("Failed to stop ssh server on port " + sshServer.getPort(), e);
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return sshServer.getPort();
    }

    public void addUser(String username, String password) {
        collectionPasswordAuthenticator.addUser(username, password);
    }

    public String getDefaultUsername() {
        return defaultUsername;
    }

    public String getDefaultPassword() {
        return defaultPassword;
    }

    public Path getPublicKeyPath() {
        return publicKeyPath;
    }

    public Path getPrivateKeyPath() {
        return privateKeyPath;
    }
}
