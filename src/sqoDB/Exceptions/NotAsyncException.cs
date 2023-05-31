﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace sqoDB.Exceptions
{
    public class NotAsyncException:Exception
    {
        public NotAsyncException():base()
		{

		}
        public NotAsyncException(string message)
            : base(message)
		{

		}
    }
}
