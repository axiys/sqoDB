using System.Collections.Generic;

namespace sqoDB
{
    /// <summary>
    ///     Main interface to be used by implementers to retrieve objects from database
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IObjectList<T> : IList<T>
    {
    }
}