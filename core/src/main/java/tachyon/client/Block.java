package tachyon.client;

import java.io.IOException;

interface Block {
  WritableBlockChannel write() throws IOException;
}
