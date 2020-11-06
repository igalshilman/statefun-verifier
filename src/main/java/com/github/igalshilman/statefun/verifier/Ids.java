package com.github.igalshilman.statefun.verifier;

import com.github.igalshilman.statefun.verifier.generated.FnAddress;

public final class Ids {
  private final String[] cache;

  public Ids(int maxIds) {
    this.cache = createIds(maxIds);
  }

  public String idOf(FnAddress address) {
    return cache[address.getId()];
  }

  private static String[] createIds(int maxIds) {
    String[] ids = new String[maxIds];
    for (int i = 0; i < maxIds; i++) {
      ids[i] = String.valueOf(i);
    }
    return ids;
  }
}
