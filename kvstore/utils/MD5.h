#include <openssl/md5.h>
//digest buffer is the output
void computeDigest(char *data, int dataLengthBytes, unsigned char *digestBuffer)
{
  /* The digest will be written to digestBuffer, which must be at least MD5_DIGEST_LENGTH bytes long */
  MD5_CTX c;
  MD5_Init(&c);
  MD5_Update(&c, data, dataLengthBytes);
  MD5_Final(digestBuffer, &c);
}

string compute_hash(string message)
{
  unsigned char* digest = new unsigned char[MD5_DIGEST_LENGTH];
  char message_c[message.length()];
  strcpy(message_c, message.c_str());
  computeDigest(message_c, strlen(message_c), digest);
  string hash;
  hash.reserve(32);
  for(int i = 0; i < 16; i++)
  {
    hash += "0123456789ABCDEF"[digest[i] / 16];
    hash += "0123456789ABCDEF"[digest[i] % 16];          
  }
  delete[] digest;
  return hash;
}
