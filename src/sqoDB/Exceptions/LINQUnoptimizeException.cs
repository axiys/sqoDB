﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace sqoDB.Exceptions
{
	public class LINQUnoptimizeException:Exception
	{
		public LINQUnoptimizeException():base()
		{

		}
		public LINQUnoptimizeException(string message)
			: base(message)
		{

		}
	}
}
