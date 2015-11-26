package com.intropro.prairie.unit.cmd;

import java.io.*;

/**
 * Created by presidentio on 11/26/15.
 */
public class StreamRedirect extends Thread {

    private BufferedReader is;
    private OutputStreamWriter os;

    private String nextLine;

    public StreamRedirect(InputStream is, OutputStream os) {
        this.is = new BufferedReader(new InputStreamReader(is));
        this.os = new OutputStreamWriter(os);
    }

    @Override
    public void run() {
        try {
            do {
                String currentLine = nextLine;
                nextLine = is.readLine();
                if (currentLine != null) {
                    handleLine(currentLine);
                }
            } while (!isEof());
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    protected void handleLine(String line) throws IOException {
        if(!isEof()) {
            line += "\n";
        }
        write(line);
    }

    protected void write(String data) throws IOException {
        os.write(data);
        os.flush();
    }

    protected boolean isEof() {
        return nextLine == null;
    }
}
