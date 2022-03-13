package com.github.cwbcheng.mapreducer;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Mobile implements WritableComparable {
    private String mobile;

    public Mobile() {

    }

    @Override
    public int compareTo(Object o) {
        Mobile m = (Mobile) o;
        long v1 = Long.valueOf(this.getMobile());
        long v2 = Long.valueOf(m.getMobile());
        long v3 = v1 - v2;
        if (v3 > 0) {
            return 1;
        } else if (v3 < 0) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeChars(mobile);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        mobile = dataInput.readLine();
    }

    public String getMobile() {
        return mobile;
    }

    public void setMobile(String mobile) {
        this.mobile = mobile;
    }
}
