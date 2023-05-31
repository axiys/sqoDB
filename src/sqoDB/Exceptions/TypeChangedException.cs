﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace sqoDB.Exceptions
{
	public class TypeChangedException : Exception
	{
		public TypeChangedException()
			: base()
		{

		}
		public TypeChangedException(string message)
			: base(message)
		{

		}
	}
}
