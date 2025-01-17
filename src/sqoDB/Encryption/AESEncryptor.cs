﻿namespace sqoDB.Encryption
{
    internal class AESEncryptor : IEncryptor
    {
        private static readonly int[] RCON = new int[10];
        private static readonly int[] FS = new int[256];
        private static readonly int[] FT0 = new int[256];
        private static readonly int[] FT1 = new int[256];
        private static readonly int[] FT2 = new int[256];
        private static readonly int[] FT3 = new int[256];
        private static readonly int[] RS = new int[256];
        private static readonly int[] RT0 = new int[256];
        private static readonly int[] RT1 = new int[256];
        private static readonly int[] RT2 = new int[256];
        private static readonly int[] RT3 = new int[256];
        private readonly int[] decKey = new int[44];
        private readonly int[] encKey = new int[44];


        static AESEncryptor()
        {
            var pow = new int[256];
            var log = new int[256];
            for (int i = 0, x = 1; i < 256; i++, x ^= xtime(x))
            {
                pow[i] = x;
                log[x] = i;
            }

            for (int i = 0, x = 1; i < 10; i++, x = xtime(x)) RCON[i] = x << 24;
            FS[0x00] = 0x63;
            RS[0x63] = 0x00;
            for (var i = 1; i < 256; i++)
            {
                int x = pow[255 - log[i]], y = x;
                y = ((y << 1) | (y >> 7)) & 255;
                x ^= y;
                y = ((y << 1) | (y >> 7)) & 255;
                x ^= y;
                y = ((y << 1) | (y >> 7)) & 255;
                x ^= y;
                y = ((y << 1) | (y >> 7)) & 255;
                x ^= y ^ 0x63;
                FS[i] = x & 255;
                RS[x] = i & 255;
            }

            for (var i = 0; i < 256; i++)
            {
                int x = FS[i], y = xtime(x);
                FT0[i] = x ^ y ^ (x << 8) ^ (x << 16) ^ (y << 24);
                FT1[i] = rot8(FT0[i]);
                FT2[i] = rot8(FT1[i]);
                FT3[i] = rot8(FT2[i]);
                y = RS[i];
                RT0[i] = mul(pow, log, 0x0b, y) ^ (mul(pow, log, 0x0d, y) << 8)
                                                ^ (mul(pow, log, 0x09, y) << 16) ^ (mul(pow, log, 0x0e, y) << 24);
                RT1[i] = rot8(RT0[i]);
                RT2[i] = rot8(RT1[i]);
                RT3[i] = rot8(RT2[i]);
            }
        }


        public AESEncryptor()
        {
            var kap = new byte[32];
            kap[0] = 54;
            kap[1] = 34;
            kap[2] = 12;
            kap[3] = 67;
            kap[4] = 87;
            kap[5] = 33;
            kap[6] = 89;
            kap[7] = 43;
            kap[8] = 45;
            kap[9] = 111;
            kap[10] = 54;
            kap[11] = 34;
            kap[12] = 12;
            kap[13] = 67;
            kap[14] = 87;
            kap[15] = 33;
            kap[16] = 89;
            kap[17] = 43;
            kap[18] = 45;
            kap[19] = 111;
            kap[20] = 54;
            kap[21] = 34;
            kap[22] = 12;
            kap[23] = 67;
            kap[24] = 87;
            kap[25] = 33;
            kap[26] = 89;
            kap[27] = 43;
            kap[28] = 45;
            kap[29] = 111;
            kap[30] = 22;
            kap[31] = 66;

            SetKey(kap);
        }


        public void Encrypt(byte[] bytes, int off, int len)
        {
            for (var i = off; i < off + len; i += 16) encryptBlock(bytes, bytes, i);
        }


        public void Decrypt(byte[] bytes, int off, int len)
        {
            for (var i = off; i < off + len; i += 16) decryptBlock(bytes, bytes, i);
        }


        public int GetBlockSize()
        {
            return 128;
        }


        private static int rot8(int x)
        {
            return (int)((uint)x >> 8) | (x << 24);
        }


        private static int xtime(int x)
        {
            return ((x << 1) ^ ((x & 0x80) != 0 ? 0x1b : 0)) & 255;
        }


        private static int mul(int[] pow, int[] log, int x, int y)
        {
            return x != 0 && y != 0 ? pow[(log[x] + log[y]) % 255] : 0;
        }


        private int getDec(int t)
        {
            return RT0[FS[(t >> 24) & 255]] ^ RT1[FS[(t >> 16) & 255]] ^ RT2[FS[(t >> 8) & 255]] ^ RT3[FS[t & 255]];
        }


        public void SetKey(byte[] key)
        {
            for (int i = 0, j = 0; i < 4; i++)
                encKey[i] = decKey[i] = ((key[j++] & 255) << 24) | ((key[j++] & 255) << 16) | ((key[j++] & 255) << 8)
                                        | (key[j++] & 255);
            var e = 0;
            for (var i = 0; i < 10; i++, e += 4)
            {
                encKey[e + 4] = encKey[e] ^ RCON[i] ^ (FS[(encKey[e + 3] >> 16) & 255] << 24)
                                ^ (FS[(encKey[e + 3] >> 8) & 255] << 16) ^ (FS[encKey[e + 3] & 255] << 8) ^
                                FS[(encKey[e + 3] >> 24) & 255];
                encKey[e + 5] = encKey[e + 1] ^ encKey[e + 4];
                encKey[e + 6] = encKey[e + 2] ^ encKey[e + 5];
                encKey[e + 7] = encKey[e + 3] ^ encKey[e + 6];
            }

            var d = 0;
            decKey[d++] = encKey[e++];
            decKey[d++] = encKey[e++];
            decKey[d++] = encKey[e++];
            decKey[d++] = encKey[e++];
            for (var i = 1; i < 10; i++)
            {
                e -= 8;
                decKey[d++] = getDec(encKey[e++]);
                decKey[d++] = getDec(encKey[e++]);
                decKey[d++] = getDec(encKey[e++]);
                decKey[d++] = getDec(encKey[e++]);
            }

            e -= 8;
            decKey[d++] = encKey[e++];
            decKey[d++] = encKey[e++];
            decKey[d++] = encKey[e++];
            decKey[d] = encKey[e];
        }


        private void encryptBlock(byte[] inBuff, byte[] outBuff, int off)
        {
            var k = encKey;
            var x0 = ((inBuff[off] << 24) | ((inBuff[off + 1] & 255) << 16) | ((inBuff[off + 2] & 255) << 8) |
                      (inBuff[off + 3] & 255)) ^ k[0];
            var x1 = ((inBuff[off + 4] << 24) | ((inBuff[off + 5] & 255) << 16) | ((inBuff[off + 6] & 255) << 8) |
                      (inBuff[off + 7] & 255)) ^ k[1];
            var x2 = ((inBuff[off + 8] << 24) | ((inBuff[off + 9] & 255) << 16) | ((inBuff[off + 10] & 255) << 8) |
                      (inBuff[off + 11] & 255)) ^ k[2];
            var x3 = ((inBuff[off + 12] << 24) | ((inBuff[off + 13] & 255) << 16) | ((inBuff[off + 14] & 255) << 8) |
                      (inBuff[off + 15] & 255)) ^ k[3];
            var y0 = FT0[(x0 >> 24) & 255] ^ FT1[(x1 >> 16) & 255] ^ FT2[(x2 >> 8) & 255] ^ FT3[x3 & 255] ^ k[4];
            var y1 = FT0[(x1 >> 24) & 255] ^ FT1[(x2 >> 16) & 255] ^ FT2[(x3 >> 8) & 255] ^ FT3[x0 & 255] ^ k[5];
            var y2 = FT0[(x2 >> 24) & 255] ^ FT1[(x3 >> 16) & 255] ^ FT2[(x0 >> 8) & 255] ^ FT3[x1 & 255] ^ k[6];
            var y3 = FT0[(x3 >> 24) & 255] ^ FT1[(x0 >> 16) & 255] ^ FT2[(x1 >> 8) & 255] ^ FT3[x2 & 255] ^ k[7];
            x0 = FT0[(y0 >> 24) & 255] ^ FT1[(y1 >> 16) & 255] ^ FT2[(y2 >> 8) & 255] ^ FT3[y3 & 255] ^ k[8];
            x1 = FT0[(y1 >> 24) & 255] ^ FT1[(y2 >> 16) & 255] ^ FT2[(y3 >> 8) & 255] ^ FT3[y0 & 255] ^ k[9];
            x2 = FT0[(y2 >> 24) & 255] ^ FT1[(y3 >> 16) & 255] ^ FT2[(y0 >> 8) & 255] ^ FT3[y1 & 255] ^ k[10];
            x3 = FT0[(y3 >> 24) & 255] ^ FT1[(y0 >> 16) & 255] ^ FT2[(y1 >> 8) & 255] ^ FT3[y2 & 255] ^ k[11];
            y0 = FT0[(x0 >> 24) & 255] ^ FT1[(x1 >> 16) & 255] ^ FT2[(x2 >> 8) & 255] ^ FT3[x3 & 255] ^ k[12];
            y1 = FT0[(x1 >> 24) & 255] ^ FT1[(x2 >> 16) & 255] ^ FT2[(x3 >> 8) & 255] ^ FT3[x0 & 255] ^ k[13];
            y2 = FT0[(x2 >> 24) & 255] ^ FT1[(x3 >> 16) & 255] ^ FT2[(x0 >> 8) & 255] ^ FT3[x1 & 255] ^ k[14];
            y3 = FT0[(x3 >> 24) & 255] ^ FT1[(x0 >> 16) & 255] ^ FT2[(x1 >> 8) & 255] ^ FT3[x2 & 255] ^ k[15];
            x0 = FT0[(y0 >> 24) & 255] ^ FT1[(y1 >> 16) & 255] ^ FT2[(y2 >> 8) & 255] ^ FT3[y3 & 255] ^ k[16];
            x1 = FT0[(y1 >> 24) & 255] ^ FT1[(y2 >> 16) & 255] ^ FT2[(y3 >> 8) & 255] ^ FT3[y0 & 255] ^ k[17];
            x2 = FT0[(y2 >> 24) & 255] ^ FT1[(y3 >> 16) & 255] ^ FT2[(y0 >> 8) & 255] ^ FT3[y1 & 255] ^ k[18];
            x3 = FT0[(y3 >> 24) & 255] ^ FT1[(y0 >> 16) & 255] ^ FT2[(y1 >> 8) & 255] ^ FT3[y2 & 255] ^ k[19];
            y0 = FT0[(x0 >> 24) & 255] ^ FT1[(x1 >> 16) & 255] ^ FT2[(x2 >> 8) & 255] ^ FT3[x3 & 255] ^ k[20];
            y1 = FT0[(x1 >> 24) & 255] ^ FT1[(x2 >> 16) & 255] ^ FT2[(x3 >> 8) & 255] ^ FT3[x0 & 255] ^ k[21];
            y2 = FT0[(x2 >> 24) & 255] ^ FT1[(x3 >> 16) & 255] ^ FT2[(x0 >> 8) & 255] ^ FT3[x1 & 255] ^ k[22];
            y3 = FT0[(x3 >> 24) & 255] ^ FT1[(x0 >> 16) & 255] ^ FT2[(x1 >> 8) & 255] ^ FT3[x2 & 255] ^ k[23];
            x0 = FT0[(y0 >> 24) & 255] ^ FT1[(y1 >> 16) & 255] ^ FT2[(y2 >> 8) & 255] ^ FT3[y3 & 255] ^ k[24];
            x1 = FT0[(y1 >> 24) & 255] ^ FT1[(y2 >> 16) & 255] ^ FT2[(y3 >> 8) & 255] ^ FT3[y0 & 255] ^ k[25];
            x2 = FT0[(y2 >> 24) & 255] ^ FT1[(y3 >> 16) & 255] ^ FT2[(y0 >> 8) & 255] ^ FT3[y1 & 255] ^ k[26];
            x3 = FT0[(y3 >> 24) & 255] ^ FT1[(y0 >> 16) & 255] ^ FT2[(y1 >> 8) & 255] ^ FT3[y2 & 255] ^ k[27];
            y0 = FT0[(x0 >> 24) & 255] ^ FT1[(x1 >> 16) & 255] ^ FT2[(x2 >> 8) & 255] ^ FT3[x3 & 255] ^ k[28];
            y1 = FT0[(x1 >> 24) & 255] ^ FT1[(x2 >> 16) & 255] ^ FT2[(x3 >> 8) & 255] ^ FT3[x0 & 255] ^ k[29];
            y2 = FT0[(x2 >> 24) & 255] ^ FT1[(x3 >> 16) & 255] ^ FT2[(x0 >> 8) & 255] ^ FT3[x1 & 255] ^ k[30];
            y3 = FT0[(x3 >> 24) & 255] ^ FT1[(x0 >> 16) & 255] ^ FT2[(x1 >> 8) & 255] ^ FT3[x2 & 255] ^ k[31];
            x0 = FT0[(y0 >> 24) & 255] ^ FT1[(y1 >> 16) & 255] ^ FT2[(y2 >> 8) & 255] ^ FT3[y3 & 255] ^ k[32];
            x1 = FT0[(y1 >> 24) & 255] ^ FT1[(y2 >> 16) & 255] ^ FT2[(y3 >> 8) & 255] ^ FT3[y0 & 255] ^ k[33];
            x2 = FT0[(y2 >> 24) & 255] ^ FT1[(y3 >> 16) & 255] ^ FT2[(y0 >> 8) & 255] ^ FT3[y1 & 255] ^ k[34];
            x3 = FT0[(y3 >> 24) & 255] ^ FT1[(y0 >> 16) & 255] ^ FT2[(y1 >> 8) & 255] ^ FT3[y2 & 255] ^ k[35];
            y0 = FT0[(x0 >> 24) & 255] ^ FT1[(x1 >> 16) & 255] ^ FT2[(x2 >> 8) & 255] ^ FT3[x3 & 255] ^ k[36];
            y1 = FT0[(x1 >> 24) & 255] ^ FT1[(x2 >> 16) & 255] ^ FT2[(x3 >> 8) & 255] ^ FT3[x0 & 255] ^ k[37];
            y2 = FT0[(x2 >> 24) & 255] ^ FT1[(x3 >> 16) & 255] ^ FT2[(x0 >> 8) & 255] ^ FT3[x1 & 255] ^ k[38];
            y3 = FT0[(x3 >> 24) & 255] ^ FT1[(x0 >> 16) & 255] ^ FT2[(x1 >> 8) & 255] ^ FT3[x2 & 255] ^ k[39];
            x0 = ((FS[(y0 >> 24) & 255] << 24) | (FS[(y1 >> 16) & 255] << 16) | (FS[(y2 >> 8) & 255] << 8) |
                  FS[y3 & 255]) ^ k[40];
            x1 = ((FS[(y1 >> 24) & 255] << 24) | (FS[(y2 >> 16) & 255] << 16) | (FS[(y3 >> 8) & 255] << 8) |
                  FS[y0 & 255]) ^ k[41];
            x2 = ((FS[(y2 >> 24) & 255] << 24) | (FS[(y3 >> 16) & 255] << 16) | (FS[(y0 >> 8) & 255] << 8) |
                  FS[y1 & 255]) ^ k[42];
            x3 = ((FS[(y3 >> 24) & 255] << 24) | (FS[(y0 >> 16) & 255] << 16) | (FS[(y1 >> 8) & 255] << 8) |
                  FS[y2 & 255]) ^ k[43];
            outBuff[off] = (byte)(x0 >> 24);
            outBuff[off + 1] = (byte)(x0 >> 16);
            outBuff[off + 2] = (byte)(x0 >> 8);
            outBuff[off + 3] = (byte)x0;
            outBuff[off + 4] = (byte)(x1 >> 24);
            outBuff[off + 5] = (byte)(x1 >> 16);
            outBuff[off + 6] = (byte)(x1 >> 8);
            outBuff[off + 7] = (byte)x1;
            outBuff[off + 8] = (byte)(x2 >> 24);
            outBuff[off + 9] = (byte)(x2 >> 16);
            outBuff[off + 10] = (byte)(x2 >> 8);
            outBuff[off + 11] = (byte)x2;
            outBuff[off + 12] = (byte)(x3 >> 24);
            outBuff[off + 13] = (byte)(x3 >> 16);
            outBuff[off + 14] = (byte)(x3 >> 8);
            outBuff[off + 15] = (byte)x3;
        }


        private void decryptBlock(byte[] inBuff, byte[] outBuff, int off)
        {
            var k = decKey;
            var x0 = ((inBuff[off] << 24) | ((inBuff[off + 1] & 255) << 16) | ((inBuff[off + 2] & 255) << 8) |
                      (inBuff[off + 3] & 255)) ^ k[0];
            var x1 = ((inBuff[off + 4] << 24) | ((inBuff[off + 5] & 255) << 16) | ((inBuff[off + 6] & 255) << 8) |
                      (inBuff[off + 7] & 255)) ^ k[1];
            var x2 = ((inBuff[off + 8] << 24) | ((inBuff[off + 9] & 255) << 16) | ((inBuff[off + 10] & 255) << 8) |
                      (inBuff[off + 11] & 255)) ^ k[2];
            var x3 = ((inBuff[off + 12] << 24) | ((inBuff[off + 13] & 255) << 16) | ((inBuff[off + 14] & 255) << 8) |
                      (inBuff[off + 15] & 255)) ^ k[3];
            var y0 = RT0[(x0 >> 24) & 255] ^ RT1[(x3 >> 16) & 255] ^ RT2[(x2 >> 8) & 255] ^ RT3[x1 & 255] ^ k[4];
            var y1 = RT0[(x1 >> 24) & 255] ^ RT1[(x0 >> 16) & 255] ^ RT2[(x3 >> 8) & 255] ^ RT3[x2 & 255] ^ k[5];
            var y2 = RT0[(x2 >> 24) & 255] ^ RT1[(x1 >> 16) & 255] ^ RT2[(x0 >> 8) & 255] ^ RT3[x3 & 255] ^ k[6];
            var y3 = RT0[(x3 >> 24) & 255] ^ RT1[(x2 >> 16) & 255] ^ RT2[(x1 >> 8) & 255] ^ RT3[x0 & 255] ^ k[7];
            x0 = RT0[(y0 >> 24) & 255] ^ RT1[(y3 >> 16) & 255] ^ RT2[(y2 >> 8) & 255] ^ RT3[y1 & 255] ^ k[8];
            x1 = RT0[(y1 >> 24) & 255] ^ RT1[(y0 >> 16) & 255] ^ RT2[(y3 >> 8) & 255] ^ RT3[y2 & 255] ^ k[9];
            x2 = RT0[(y2 >> 24) & 255] ^ RT1[(y1 >> 16) & 255] ^ RT2[(y0 >> 8) & 255] ^ RT3[y3 & 255] ^ k[10];
            x3 = RT0[(y3 >> 24) & 255] ^ RT1[(y2 >> 16) & 255] ^ RT2[(y1 >> 8) & 255] ^ RT3[y0 & 255] ^ k[11];
            y0 = RT0[(x0 >> 24) & 255] ^ RT1[(x3 >> 16) & 255] ^ RT2[(x2 >> 8) & 255] ^ RT3[x1 & 255] ^ k[12];
            y1 = RT0[(x1 >> 24) & 255] ^ RT1[(x0 >> 16) & 255] ^ RT2[(x3 >> 8) & 255] ^ RT3[x2 & 255] ^ k[13];
            y2 = RT0[(x2 >> 24) & 255] ^ RT1[(x1 >> 16) & 255] ^ RT2[(x0 >> 8) & 255] ^ RT3[x3 & 255] ^ k[14];
            y3 = RT0[(x3 >> 24) & 255] ^ RT1[(x2 >> 16) & 255] ^ RT2[(x1 >> 8) & 255] ^ RT3[x0 & 255] ^ k[15];
            x0 = RT0[(y0 >> 24) & 255] ^ RT1[(y3 >> 16) & 255] ^ RT2[(y2 >> 8) & 255] ^ RT3[y1 & 255] ^ k[16];
            x1 = RT0[(y1 >> 24) & 255] ^ RT1[(y0 >> 16) & 255] ^ RT2[(y3 >> 8) & 255] ^ RT3[y2 & 255] ^ k[17];
            x2 = RT0[(y2 >> 24) & 255] ^ RT1[(y1 >> 16) & 255] ^ RT2[(y0 >> 8) & 255] ^ RT3[y3 & 255] ^ k[18];
            x3 = RT0[(y3 >> 24) & 255] ^ RT1[(y2 >> 16) & 255] ^ RT2[(y1 >> 8) & 255] ^ RT3[y0 & 255] ^ k[19];
            y0 = RT0[(x0 >> 24) & 255] ^ RT1[(x3 >> 16) & 255] ^ RT2[(x2 >> 8) & 255] ^ RT3[x1 & 255] ^ k[20];
            y1 = RT0[(x1 >> 24) & 255] ^ RT1[(x0 >> 16) & 255] ^ RT2[(x3 >> 8) & 255] ^ RT3[x2 & 255] ^ k[21];
            y2 = RT0[(x2 >> 24) & 255] ^ RT1[(x1 >> 16) & 255] ^ RT2[(x0 >> 8) & 255] ^ RT3[x3 & 255] ^ k[22];
            y3 = RT0[(x3 >> 24) & 255] ^ RT1[(x2 >> 16) & 255] ^ RT2[(x1 >> 8) & 255] ^ RT3[x0 & 255] ^ k[23];
            x0 = RT0[(y0 >> 24) & 255] ^ RT1[(y3 >> 16) & 255] ^ RT2[(y2 >> 8) & 255] ^ RT3[y1 & 255] ^ k[24];
            x1 = RT0[(y1 >> 24) & 255] ^ RT1[(y0 >> 16) & 255] ^ RT2[(y3 >> 8) & 255] ^ RT3[y2 & 255] ^ k[25];
            x2 = RT0[(y2 >> 24) & 255] ^ RT1[(y1 >> 16) & 255] ^ RT2[(y0 >> 8) & 255] ^ RT3[y3 & 255] ^ k[26];
            x3 = RT0[(y3 >> 24) & 255] ^ RT1[(y2 >> 16) & 255] ^ RT2[(y1 >> 8) & 255] ^ RT3[y0 & 255] ^ k[27];
            y0 = RT0[(x0 >> 24) & 255] ^ RT1[(x3 >> 16) & 255] ^ RT2[(x2 >> 8) & 255] ^ RT3[x1 & 255] ^ k[28];
            y1 = RT0[(x1 >> 24) & 255] ^ RT1[(x0 >> 16) & 255] ^ RT2[(x3 >> 8) & 255] ^ RT3[x2 & 255] ^ k[29];
            y2 = RT0[(x2 >> 24) & 255] ^ RT1[(x1 >> 16) & 255] ^ RT2[(x0 >> 8) & 255] ^ RT3[x3 & 255] ^ k[30];
            y3 = RT0[(x3 >> 24) & 255] ^ RT1[(x2 >> 16) & 255] ^ RT2[(x1 >> 8) & 255] ^ RT3[x0 & 255] ^ k[31];
            x0 = RT0[(y0 >> 24) & 255] ^ RT1[(y3 >> 16) & 255] ^ RT2[(y2 >> 8) & 255] ^ RT3[y1 & 255] ^ k[32];
            x1 = RT0[(y1 >> 24) & 255] ^ RT1[(y0 >> 16) & 255] ^ RT2[(y3 >> 8) & 255] ^ RT3[y2 & 255] ^ k[33];
            x2 = RT0[(y2 >> 24) & 255] ^ RT1[(y1 >> 16) & 255] ^ RT2[(y0 >> 8) & 255] ^ RT3[y3 & 255] ^ k[34];
            x3 = RT0[(y3 >> 24) & 255] ^ RT1[(y2 >> 16) & 255] ^ RT2[(y1 >> 8) & 255] ^ RT3[y0 & 255] ^ k[35];
            y0 = RT0[(x0 >> 24) & 255] ^ RT1[(x3 >> 16) & 255] ^ RT2[(x2 >> 8) & 255] ^ RT3[x1 & 255] ^ k[36];
            y1 = RT0[(x1 >> 24) & 255] ^ RT1[(x0 >> 16) & 255] ^ RT2[(x3 >> 8) & 255] ^ RT3[x2 & 255] ^ k[37];
            y2 = RT0[(x2 >> 24) & 255] ^ RT1[(x1 >> 16) & 255] ^ RT2[(x0 >> 8) & 255] ^ RT3[x3 & 255] ^ k[38];
            y3 = RT0[(x3 >> 24) & 255] ^ RT1[(x2 >> 16) & 255] ^ RT2[(x1 >> 8) & 255] ^ RT3[x0 & 255] ^ k[39];
            x0 = ((RS[(y0 >> 24) & 255] << 24) | (RS[(y3 >> 16) & 255] << 16) | (RS[(y2 >> 8) & 255] << 8) |
                  RS[y1 & 255]) ^ k[40];
            x1 = ((RS[(y1 >> 24) & 255] << 24) | (RS[(y0 >> 16) & 255] << 16) | (RS[(y3 >> 8) & 255] << 8) |
                  RS[y2 & 255]) ^ k[41];
            x2 = ((RS[(y2 >> 24) & 255] << 24) | (RS[(y1 >> 16) & 255] << 16) | (RS[(y0 >> 8) & 255] << 8) |
                  RS[y3 & 255]) ^ k[42];
            x3 = ((RS[(y3 >> 24) & 255] << 24) | (RS[(y2 >> 16) & 255] << 16) | (RS[(y1 >> 8) & 255] << 8) |
                  RS[y0 & 255]) ^ k[43];
            outBuff[off] = (byte)(x0 >> 24);
            outBuff[off + 1] = (byte)(x0 >> 16);
            outBuff[off + 2] = (byte)(x0 >> 8);
            outBuff[off + 3] = (byte)x0;
            outBuff[off + 4] = (byte)(x1 >> 24);
            outBuff[off + 5] = (byte)(x1 >> 16);
            outBuff[off + 6] = (byte)(x1 >> 8);
            outBuff[off + 7] = (byte)x1;
            outBuff[off + 8] = (byte)(x2 >> 24);
            outBuff[off + 9] = (byte)(x2 >> 16);
            outBuff[off + 10] = (byte)(x2 >> 8);
            outBuff[off + 11] = (byte)x2;
            outBuff[off + 12] = (byte)(x3 >> 24);
            outBuff[off + 13] = (byte)(x3 >> 16);
            outBuff[off + 14] = (byte)(x3 >> 8);
            outBuff[off + 15] = (byte)x3;
        }
    }
}