package com.mapreduce2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StudentWritable implements WritableComparable<StudentWritable> {

    long math;
    long english;

    public StudentWritable() {
    }

    public StudentWritable(long math, long english) {
        this.math = math;
        this.english = english;
    }

    public long getMath() {
        return math;
    }

    public void setMath(long math) {
        this.math = math;
    }

    public long getEnglish() {
        return english;
    }

    public void setEnglish(long english) {
        this.english = english;
    }

    @Override
    public String toString() {
        return  math + " " + english;
    }

    @Override
    public int compareTo(StudentWritable o) {
        long result = this.math - o.math;
        if (result != 0) {
            return (int) result;
        } else {
            return (int) (this.english - o.english);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(math);
        out.writeLong(english);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.math = in.readLong();
        this.english = in.readLong();
    }
}
