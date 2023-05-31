using System;
using sqoDB.Meta;

namespace sqoDB.Utilities
{
    internal class ObjectTableHelper
    {
        public static ObjectList<T> CreateObjectsFromTable<T>(ObjectTable table, SqoTypeInfo actualType)
        {
            var obList = new ObjectList<T>();
            foreach (var row in table.Rows)
            {
                var currentObj = default(T);
                currentObj = Activator.CreateInstance<T>();
                //ISqoDataObject dObj = currentObj as ISqoDataObject;

                foreach (var column in table.Columns.Keys)
                {
                    var fi = MetaHelper.FindField(actualType.Fields, column);
                    if (fi != null)
                    {
#if SILVERLIGHT
                        try
                            {
                                //dObj.SetValue(fi.FInfo, row[column]);
                                MetaHelper.CallSetValue(fi.FInfo, row[column], currentObj, actualType.Type);
                                
                            }
                            catch (Exception ex)
                            {
                                throw new SiaqodbException("Override GetValue and SetValue methods of SqoDataObject-Silverlight limitation to private fields");
                            }

#else
                        fi.FInfo.SetValue(currentObj, row[column]);
#endif
                    }
                }


                obList.Add(currentObj);
            }

            return obList;
        }
    }
}