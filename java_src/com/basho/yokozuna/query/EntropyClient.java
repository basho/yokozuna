/**
 * Lucene index walker client.
 *
 */




package com.basho.yokozuna.query;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;


import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;

public class EntropyClient {
  public static void main(String... args) throws IOException {

    String dirS = args[0];
    Path pa = FileSystems.getDefault().getPath(dirS);
    Directory dir = new MMapDirectory(pa);

    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      reader.leaves().stream().forEachOrdered((LeafReaderContext x) -> {
        System.out.printf("\u001b[4m=== %s\u001b[m\nbase=%d\n", x, x.docBase);
      });
    }

    System.err.println("Done.");

  }
}
