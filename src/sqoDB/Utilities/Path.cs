namespace sqoDB

{
    internal class Path
    {
        public static char DirectorySeparatorChar => '\\';

        internal static string GetDirectoryName(string fullPath)
        {
            return fullPath.Remove(fullPath.LastIndexOf('\\'));
        }

        internal static string GetFileName(string fullPath)
        {
            return fullPath.Substring(fullPath.LastIndexOf('\\') + 1);
        }
    }
}