﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace sqoDB.Exceptions
{
	public class NotSupportedTypeException:Exception
	{
		public NotSupportedTypeException():base()
		{

		}
		public NotSupportedTypeException(string message):base(message)
		{

		}
	}
}
