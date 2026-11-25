package com.raftis.store;

/** Redis-style glob matching for KEYS and CONFIG GET: {@code * ? [abc] [^a] [a-z] \x}. */
public final class Glob {
    private Glob() {}

    public static boolean match(byte[] pattern, byte[] string) {
        return match(pattern, 0, string, 0);
    }

    private static boolean match(byte[] p, int pi, byte[] s, int si) {
        while (pi < p.length) {
            byte c = p[pi];
            switch (c) {
                case '*' -> {
                    while (pi + 1 < p.length && p[pi + 1] == '*') pi++;
                    if (pi + 1 == p.length) return true;
                    for (int k = si; k <= s.length; k++) {
                        if (match(p, pi + 1, s, k)) return true;
                    }
                    return false;
                }
                case '?' -> {
                    if (si >= s.length) return false;
                    pi++;
                    si++;
                }
                case '[' -> {
                    if (si >= s.length) return false;
                    int j = pi + 1;
                    boolean negate = j < p.length && p[j] == '^';
                    if (negate) j++;
                    boolean matched = false;
                    int ch = s[si] & 0xff;
                    while (j < p.length && p[j] != ']') {
                        if (p[j] == '\\' && j + 1 < p.length) {
                            if ((p[j + 1] & 0xff) == ch) matched = true;
                            j += 2;
                        } else if (j + 2 < p.length && p[j + 1] == '-' && p[j + 2] != ']') {
                            int lo = p[j] & 0xff;
                            int hi = p[j + 2] & 0xff;
                            if (ch >= Math.min(lo, hi) && ch <= Math.max(lo, hi)) matched = true;
                            j += 3;
                        } else {
                            if ((p[j] & 0xff) == ch) matched = true;
                            j++;
                        }
                    }
                    if (matched == negate) return false;
                    pi = j < p.length ? j + 1 : j;
                    si++;
                }
                case '\\' -> {
                    if (pi + 1 < p.length) pi++;
                    if (si >= s.length || p[pi] != s[si]) return false;
                    pi++;
                    si++;
                }
                default -> {
                    if (si >= s.length || c != s[si]) return false;
                    pi++;
                    si++;
                }
            }
        }
        return si == s.length;
    }
}
