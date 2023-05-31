﻿using System;

namespace sqoDB.Core
{
    internal interface ISqoFile
    {

         void Write(long pos, byte[] buf);

         void Write(byte[] buf);
         int Read(long pos, byte[] buf);
         
         bool IsClosed { get; }

         void Flush();


         void Close();

         long Length { get; set; }


#if ASYNC
         System.Threading.Tasks.Task WriteAsync(long pos, byte[] buf);
         System.Threading.Tasks.Task WriteAsync(byte[] buf);
         System.Threading.Tasks.Task <int> ReadAsync(long pos, byte[] buf);
         System.Threading.Tasks.Task FlushAsync();
#endif


    }
}
