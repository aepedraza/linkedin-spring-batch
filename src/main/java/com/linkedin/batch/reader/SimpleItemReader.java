package com.linkedin.batch.reader;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemReaderException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SimpleItemReader implements ItemReader<String> {

    private final Iterator<String> iterator;

    public SimpleItemReader() {
        List<String> dataSet = new ArrayList<>();
        dataSet.add("1");
        dataSet.add("2");
        dataSet.add("3");
        dataSet.add("4");
        dataSet.add("5");
        iterator = dataSet.iterator();
    }

    @Override
    public String read() throws ItemReaderException {
        // this method is called over and over until it returns null, signaling there is nothing else to read
        return iterator.hasNext() ? iterator.next() : null;
    }
}
