package com.intropro.prairie.format.binary;

import com.intropro.prairie.format.Format;
import com.intropro.prairie.format.InputFormatReader;
import com.intropro.prairie.format.OutputFormatWriter;
import com.intropro.prairie.format.exception.FormatException;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by presidentio on 8/19/16.
 */
public class BinaryFormat implements Format<byte[]> {
    @Override
    public InputFormatReader<byte[]> createReader(InputStream inputStream) throws FormatException {
        return new BinaryFormatReader(inputStream);
    }

    @Override
    public OutputFormatWriter<byte[]> createWriter(OutputStream outputStream) throws FormatException {
        return new BinaryFormatWriter(outputStream);
    }
}
