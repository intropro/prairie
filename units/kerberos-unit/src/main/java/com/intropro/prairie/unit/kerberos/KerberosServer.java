package com.intropro.prairie.unit.kerberos;

import com.intropro.prairie.unit.kerberos.exception.KerberosException;
import org.apache.directory.api.ldap.model.entry.Entry;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.exception.LdapInvalidDnException;
import org.apache.directory.api.ldap.model.name.Dn;
import org.apache.directory.api.ldap.model.schema.SchemaManager;
import org.apache.directory.api.ldap.model.schema.registries.SchemaLoader;
import org.apache.directory.api.ldap.schema.extractor.SchemaLdifExtractor;
import org.apache.directory.api.ldap.schema.extractor.impl.DefaultSchemaLdifExtractor;
import org.apache.directory.api.ldap.schema.loader.LdifSchemaLoader;
import org.apache.directory.api.ldap.schema.manager.impl.DefaultSchemaManager;
import org.apache.directory.api.util.exception.Exceptions;
import org.apache.directory.server.constants.ServerDNConstants;
import org.apache.directory.server.core.DefaultDirectoryService;
import org.apache.directory.server.core.api.CacheService;
import org.apache.directory.server.core.api.DirectoryService;
import org.apache.directory.server.core.api.DnFactory;
import org.apache.directory.server.core.api.InstanceLayout;
import org.apache.directory.server.core.api.partition.Partition;
import org.apache.directory.server.core.api.schema.SchemaPartition;
import org.apache.directory.server.core.kerberos.KeyDerivationInterceptor;
import org.apache.directory.server.core.partition.impl.btree.jdbm.JdbmIndex;
import org.apache.directory.server.core.partition.impl.btree.jdbm.JdbmPartition;
import org.apache.directory.server.core.partition.ldif.LdifPartition;
import org.apache.directory.server.i18n.I18n;
import org.apache.directory.server.kerberos.KerberosConfig;
import org.apache.directory.server.kerberos.kdc.KdcServer;
import org.apache.directory.server.protocol.shared.transport.TcpTransport;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by presidentio on 8/29/16.
 */
public class KerberosServer {

    private static final Logger LOGGER = LogManager.getLogger(KerberosServer.class);

    private static final String PARTITION_DC = "dc=prairie";
    private static final String USERS_DN = "ou=users," + PARTITION_DC;

    private DirectoryService directoryService;
    private KdcServer kdcServer;

    private Path dataDir;
    private String realm;
    private String host = "localhost";
    private int port;

    public KerberosServer(Path dataDir, String realm, int port) {
        this.dataDir = dataDir;
        this.realm = realm;
        this.port = port;
    }

    public void init() throws KerberosException {
        File workingDirectory = dataDir.resolve("work").toFile();
        workingDirectory.mkdirs();
        initDirectoryService(workingDirectory);
        initKdcServer();
        LOGGER.info("Starting kerberos server started on " + host + ":" + port);
        new Thread() {
            @Override
            public void run() {
                try {
                    kdcServer.start();
                } catch (IOException | LdapInvalidDnException e) {
                    LOGGER.error("Failed to start kdc server", e);
                }
            }
        }.start();
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getPrincipal(String username) {
        return username + "@" + realm;
    }

    public void addUserEntry(String username, String password) throws KerberosException {
        String entryDN = "uid=" + username + "," + USERS_DN;
        try {
            Entry entry = directoryService.newEntry(new Dn(entryDN));
            entry.add("objectClass", directoryService.getAtProvider().getObjectClass(), "inetOrgPerson",
                    "organizationalPerson", "person", "krb5principal", "krb5kdcentry", "top");
            entry.add("uid", getPrincipal(username));
            entry.add("cn", username);
            entry.add("sn", username);
            entry.add("userPassword", password);
            entry.add("krb5PrincipalName", getPrincipal(username));
            entry.add("krb5KeyVersionNumber", "0");
            directoryService.getAdminSession().add(entry);
        } catch (LdapException e) {
            throw new KerberosException("Failed to add user username: " + username + ", password: " + password, e);
        }
    }

    private void initKdcServer() throws KerberosException {
        KerberosConfig kdcConfig = new KerberosConfig();
        kdcConfig.setSearchBaseDn(USERS_DN);
        kdcConfig.setServicePrincipal(getPrincipal("krbtgt/" + realm));
        kdcConfig.setPrimaryRealm(realm);
        kdcConfig.setMaximumTicketLifetime(TimeUnit.DAYS.toMillis(1));
        kdcConfig.setMaximumRenewableLifetime(TimeUnit.DAYS.toMillis(1));
        kdcConfig.setForwardableAllowed(true);
        kdcConfig.setPostdatedAllowed(true);
        kdcConfig.setProxiableAllowed(true);
        kdcConfig.setRenewableAllowed(true);
        kdcConfig.setEmptyAddressesAllowed(true);
        kdcConfig.setPaEncTimestampRequired(false);
        kdcServer = new KdcServer(kdcConfig);
        try {
            host = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            LOGGER.warn("Failed to get canonical host name. Using default: " + host);
        }
        TcpTransport defaultTransport = new TcpTransport(host, port);
        kdcServer.addTransports(defaultTransport);
        kdcServer.setDirectoryService(directoryService);
        addUserEntry("krbtgt/" + realm, "pass");
    }

    private void initDirectoryService(File workDir) throws KerberosException {
        try {
            directoryService = new DefaultDirectoryService();
        } catch (Exception e) {
            throw new KerberosException("Failed to create directory service", e);
        }
        try {
            directoryService.setInstanceLayout(new InstanceLayout(workDir));
        } catch (IOException e) {
            throw new KerberosException(e);
        }

        CacheService cacheService = new CacheService();
        cacheService.initialize(directoryService.getInstanceLayout());
        directoryService.setCacheService(cacheService);

        // first load the schema
        try {
            initSchemaPartition();
        } catch (LdapException | IOException e) {
            throw new KerberosException(e);
        }

        // then the system partition
        // this is a MANDATORY partition
        JdbmPartition systemPartition = new JdbmPartition(directoryService.getSchemaManager(), directoryService.getDnFactory());
        systemPartition.setId("system");
        systemPartition.setPartitionPath(new File(directoryService.getInstanceLayout().getPartitionsDirectory(), systemPartition.getId()).toURI());
        try {
            systemPartition.setSuffixDn(new Dn(ServerDNConstants.SYSTEM_DN));
        } catch (LdapInvalidDnException e) {
            throw new KerberosException(e);
        }
        systemPartition.setSchemaManager(directoryService.getSchemaManager());
        directoryService.setSystemPartition(systemPartition);

        // Disable the ChangeLog system
        directoryService.getChangeLog().setEnabled(false);
        directoryService.setDenormalizeOpAttrsEnabled(true);

        // Now we can create as many partitions as we need
        Partition prairiePartition;
        try {
            prairiePartition = addPartition("prairie", PARTITION_DC, directoryService.getDnFactory());
            addIndex(prairiePartition, "objectClass", "ou", "uid");
        } catch (Exception e) {
            throw new KerberosException("Failed to add partition", e);
        }


        try {
            directoryService.addFirst(new KeyDerivationInterceptor());
        } catch (LdapException e) {
            throw new KerberosException("Failed to add KeyDerivationInterceptor", e);
        }

        // And start the service
        try {
            directoryService.startup();
        } catch (Exception e) {
            throw new KerberosException("Failed to start directory service", e);
        }

        // Inject the context entry for dc=prairie partition
        try {
            if (!directoryService.getAdminSession().exists(prairiePartition.getSuffixDn())) {
                Dn dnApache = new Dn(PARTITION_DC);
                Entry entryApache = directoryService.newEntry(dnApache);
                entryApache.add("objectClass", "top", "domain", "extensibleObject");
                entryApache.add("dc", "prairie");
                directoryService.getAdminSession().add(entryApache);
                Entry entry = directoryService.newEntry(new Dn(USERS_DN));
                entry.add("objectClass", directoryService.getAtProvider().getObjectClass(), "organizationalUnit", "top");
                entry.add("ou", "users");
                directoryService.getAdminSession().add(entry);
            }
        } catch (LdapException e) {
            throw new KerberosException(e);
        }
    }

    private void initSchemaPartition() throws IOException, LdapException, KerberosException {
        InstanceLayout instanceLayout = directoryService.getInstanceLayout();
        File schemaPartitionDirectory = new File(instanceLayout.getPartitionsDirectory(), "schema");
        // Extract the schema on disk (a brand new one) and load the registries
        if (schemaPartitionDirectory.exists()) {
            LOGGER.info("schema partition already exists, skipping schema extraction");
        } else {
            SchemaLdifExtractor extractor = new DefaultSchemaLdifExtractor(instanceLayout.getPartitionsDirectory());
            extractor.extractOrCopy();
        }
        SchemaLoader loader = new LdifSchemaLoader(schemaPartitionDirectory);
        SchemaManager schemaManager = new DefaultSchemaManager(loader);
        // We have to load the schema now, otherwise we won't be able
        // to initialize the Partitions, as we won't be able to parse
        // and normalize their suffix Dn
        schemaManager.loadAllEnabled();
        List<Throwable> errors = schemaManager.getErrors();
        if (errors.size() != 0) {
            throw new KerberosException(I18n.err(I18n.ERR_317, Exceptions.printErrors(errors)));
        }
        directoryService.setSchemaManager(schemaManager);
        // Init the LdifPartition with schema
        LdifPartition schemaLdifPartition = new LdifPartition(schemaManager, directoryService.getDnFactory());
        schemaLdifPartition.setPartitionPath(schemaPartitionDirectory.toURI());
        // The schema partition
        SchemaPartition schemaPartition = new SchemaPartition(schemaManager);
        schemaPartition.setWrappedPartition(schemaLdifPartition);
        directoryService.setSchemaPartition(schemaPartition);
    }

    private Partition addPartition(String partitionId, String partitionDn, DnFactory dnFactory) throws Exception {
        JdbmPartition partition = new JdbmPartition(directoryService.getSchemaManager(), dnFactory);
        partition.setId(partitionId);
        partition.setPartitionPath(new File(directoryService.getInstanceLayout().getPartitionsDirectory(), partitionId)
                .toURI());
        partition.setSuffixDn(new Dn(directoryService.getSchemaManager(), partitionDn));
        directoryService.addPartition(partition);
        return partition;
    }

    private void addIndex(Partition partition, String... attrs) {
        Set indexedAttributes = new HashSet();
        for (String attribute : attrs) {
            indexedAttributes.add(new JdbmIndex<>(attribute, false));
        }
        ((JdbmPartition) partition).setIndexedAttributes(indexedAttributes);
    }

    public void destroy() throws KerberosException {
        kdcServer.stop();
        try {
            directoryService.shutdown();
        } catch (Exception e) {
            throw new KerberosException("Failed to shutdown directory service", e);
        }
    }

}
