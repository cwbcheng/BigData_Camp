package com.github.cwbcheng.mapreducer;


import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Traffic implements Writable {
    // 上行流量
    private long upload;
    // 下行流量
    private long download;

    public Traffic() {
        super();
    }

    public long getUpload() {
        return upload;
    }

    public void setUpload(long upload) {
        this.upload = upload;
    }

    public long getDownload() {
        return download;
    }

    public void setDownload(long download) {
        this.download = download;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upload);
        dataOutput.writeLong(download);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upload = dataInput.readLong();
        download = dataInput.readLong();
    }

    @Override
    public String toString() {
        return upload + "\t" + download + "\t" + (upload + download);
    }
}
