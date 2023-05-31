using System;
using System.Collections.Generic;
using System.Xml;
using sqoDB.Exceptions;
using sqoDB.Meta;

namespace sqoDB.Utilities
{
#if !UNITY3D
    internal class ImportExport
    {
        public static void ExportToXML<T>(XmlWriter writer, IList<T> objects, Siaqodb siaqodb)
        {
            var ti = siaqodb.GetSqoTypeInfo<T>();

            writer.WriteStartElement("SiaqodbObjects");

            writer.WriteStartElement("TypeDefinition");
            foreach (var fi in ti.Fields)
            {
                writer.WriteStartElement("member");
                writer.WriteAttributeString("type", fi.AttributeType.FullName);
                writer.WriteString(fi.Name);
                writer.WriteEndElement();
            }

            writer.WriteEndElement();
            writer.WriteStartElement("objects");
            foreach (var obj in objects)
            {
                writer.WriteStartElement("object");
                var oi = MetaExtractor.GetObjectInfo(obj, ti, siaqodb.metaCache);
                foreach (var fi in ti.Fields)
                {
                    writer.WriteStartElement("memberValue");
                    var typeElement = fi.AttributeType;
                    if (typeElement == typeof(char))
                    {
                        writer.WriteValue(oi.AtInfo[fi].ToString());
                    }
                    else if (typeElement == typeof(Guid))
                    {
                        writer.WriteValue(oi.AtInfo[fi].ToString());
                    }
                    else if (typeElement.IsEnum())
                    {
                        //writer.WriteValue(oi.AtInfo[fi]);
                        var enumType = Enum.GetUnderlyingType(typeElement);

                        var realObject = Convertor.ChangeType(oi.AtInfo[fi], enumType);

                        writer.WriteValue(realObject);
                    }
                    else if (oi.AtInfo[fi] != null && oi.AtInfo[fi].GetType().IsEnum())
                    {
                        var enumType = Enum.GetUnderlyingType(oi.AtInfo[fi].GetType());
                        var realObject = Convertor.ChangeType(oi.AtInfo[fi], enumType);
                        writer.WriteValue(realObject);
                    }
                    else
                    {
                        if (oi.AtInfo[fi] != null) writer.WriteValue(oi.AtInfo[fi]);
                    }

                    writer.WriteEndElement();
                }

                writer.WriteEndElement();
            }

            writer.WriteEndElement();
            writer.WriteEndElement();
        }

        public static IObjectList<T> ImportFromXML<T>(XmlReader reader, Siaqodb siaqodb)
        {
            var obTable = new ObjectTable();
            reader.Read();
            reader.ReadStartElement("SiaqodbObjects");
            var ti = siaqodb.GetSqoTypeInfo<T>();
            var colFinish = false;
            ObjectRow currentRow = null;
            var index = 0;
            var members = new Dictionary<int, Type>();
            while (reader.Read())
            {
                if (reader.IsStartElement() && reader.Name == "objects") colFinish = true;

                if (reader.IsStartElement() && !colFinish)
                {
                    reader.MoveToFirstAttribute();
                    //string type = reader.Value;
                    var t = Type.GetType(reader.Value);
                    reader.MoveToElement();

                    reader.ReadStartElement();
                    var columnName = reader.ReadContentAsString();
                    if (columnName == "OID")
                        throw new SiaqodbException("OID is set only internally, cannot be imported");
                    obTable.Columns.Add(columnName, index);
                    if (t.IsGenericType())
                    {
                        var genericTypeDef = t.GetGenericTypeDefinition();
                        if (genericTypeDef == typeof(Nullable<>)) t = t.GetGenericArguments()[0];
                    }

                    members.Add(index, t);
                    index++;
                }

                if (reader.IsStartElement() && reader.Name == "object")
                {
                    currentRow = obTable.NewRow();
                    obTable.Rows.Add(currentRow);
                    index = 0;
                }

                if (reader.IsStartElement() && reader.Name == "memberValue")
                {
                    ReadMemberValue(currentRow, reader, index, members);
                    index++;
                    while (reader.Name == "memberValue")
                    {
                        ReadMemberValue(currentRow, reader, index, members);
                        index++;
                    }
                }
            }

            return ObjectTableHelper.CreateObjectsFromTable<T>(obTable, ti);
        }

        private static void ReadMemberValue(ObjectRow currentRow, XmlReader reader, int index,
            Dictionary<int, Type> members)
        {
            if (!reader.IsEmptyElement)
            {
                if (members[index] == typeof(char))
                {
                    var s = reader.ReadElementContentAsString();
                    if (!string.IsNullOrEmpty(s)) currentRow[index] = s[0];
                }
                else if (members[index] == typeof(Guid))
                {
                    var s = reader.ReadElementContentAsString();

                    currentRow[index] = new Guid(s);
                }
                else if (members[index].IsEnum())
                {
                    var s = reader.ReadElementContentAsString();

                    var enumType = Enum.GetUnderlyingType(members[index]);

                    var realObject = Convertor.ChangeType(s, enumType);

                    currentRow[index] = Enum.ToObject(members[index], realObject);
                }
                else
                {
                    currentRow[index] = reader.ReadElementContentAs(members[index], null);
                }
            }
            else
            {
                reader.ReadElementContentAsString();
            }
            //reader.MoveToElement();
        }
    }
#endif
}