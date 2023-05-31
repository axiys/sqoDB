using System;


namespace sqoDB.Exceptions
{
    public class InvalidLicenseException:Exception
    {
        public InvalidLicenseException():base()
		{

		}
        public InvalidLicenseException(string message)
            : base(message)
		{

		}
    }
}
