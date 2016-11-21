// Given separator, string, start and end fields, calculate substring start and end positions and write them to array
int c_subrec (unsigned char sep, char *s, long off, long len, long start_field, long end_field, long *out) {
      long i = 0, k = 0;
      s += off;
      while (k < start_field && i < len) {
        if (s[i++] == sep) k++;
      }
      long i1 = i;
      while (k <= end_field && i < len) {
        if (s[i++] == sep) k++;
      }
      out[0] = off + i1; // substring offset
      out[1] = i == len && k <= end_field ? i - i1 : i - i1 - 1; //substring length
      return 0;
}
