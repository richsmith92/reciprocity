
int c_subrec (unsigned char sep, char *s, long bs_len, long start, long end, long *mv) {

      long i = 0, k = 0;
      while (k < start && i < bs_len) {
        if (s[i++] == sep) k++;
      }
      long i1 = i;
      while (k <= end && i < bs_len) {
        if (s[i++] == sep) k++;
      }
      mv[0] = i1;
      mv[1] = i == bs_len && k <= end ? i - i1 : i - i1 - 1;
      return 0;

}
