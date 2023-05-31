using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Windows.Forms;
using sqoDB.Internal;

namespace sqoDB.Manager
{
    public partial class UCObjects : UserControl
    {
        private Dictionary<int, MetaType> columnsTypes = new Dictionary<int, MetaType>();
        private MetaType metaType;
        private List<int> oids;
        private Siaqodb siaqodb;
        private List<MetaType> typesList;

        public UCObjects()
        {
            InitializeComponent();
        }

        internal event EventHandler<MetaEventArgs> OpenObjects;

        internal void Initialize(MetaType metaType, Siaqodb siaqodb, List<MetaType> typesList)
        {
            Initialize(metaType, siaqodb, typesList, null);
        }

        internal void Initialize(MetaType metaType, Siaqodb siaqodb, List<MetaType> typesList, List<int> oidsFiltered)
        {
            this.metaType = metaType;
            this.siaqodb = siaqodb;
            this.typesList = typesList;

            if (oidsFiltered == null)
                oids = siaqodb.LoadAllOIDs(metaType);
            else
                oids = oidsFiltered;
            if (oids == null) MessageBox.Show("FileName of this Type has not default name of siaqodb database file!");
            dataGridView1.Columns.Clear();
            dataGridView1.Columns.Add("OID", "OID");
            foreach (var f in metaType.Fields)
                if (typeof(IList).IsAssignableFrom(f.FieldType))
                {
                    var column = new DataGridViewLinkColumn();
                    column.Name = f.Name;
                    column.HeaderText = f.Name;
                    column.ValueType = f.FieldType;
                    dataGridView1.Columns.Add(column);
                }
                else if (f.FieldType == null) //complex type
                {
                    var column = new DataGridViewLinkColumn();
                    column.Name = f.Name;
                    column.HeaderText = f.Name;
                    column.ValueType = typeof(string);
                    dataGridView1.Columns.Add(column);
                }
                else
                {
                    dataGridView1.Columns.Add(f.Name, f.Name);
                }
        }

        private void UCObjects_Load(object sender, EventArgs e)
        {
            dataGridView1.VirtualMode = true;
            if (oids != null) dataGridView1.RowCount = oids.Count + 1;
            if (oids != null) lblNrRows.Text = oids.Count + " rows";
        }

        protected void OnOpenObjects(MetaEventArgs args)
        {
            if (OpenObjects != null) OpenObjects(this, args);
        }

        private void dataGridView1_CellDoubleClick(object sender, DataGridViewCellEventArgs e)
        {
            if (e.RowIndex > 0)
                if (dataGridView1.Columns[e.ColumnIndex] is DataGridViewLinkColumn)
                {
                    if (dataGridView1.Columns[e.ColumnIndex].ValueType == typeof(string))
                        EditComplexObject(e.RowIndex, e.ColumnIndex);
                    else
                        EditArray(e.RowIndex, e.ColumnIndex);
                }
        }

        private void dataGridView1_CellContentClick(object sender, DataGridViewCellEventArgs e)
        {
            //click on array
            if (e.RowIndex >= 0)
                if (dataGridView1.Columns[e.ColumnIndex] is DataGridViewLinkColumn)
                {
                    if (dataGridView1.Columns[e.ColumnIndex].ValueType == typeof(string))
                        EditComplexObject(e.RowIndex, e.ColumnIndex);
                    else
                        EditArray(e.RowIndex, e.ColumnIndex);
                }
        }

        private void EditComplexObject(int rowIndex, int columnIndex)
        {
            var fi = metaType.Fields[columnIndex - 1];

            var oids = new List<int>();
            var TID = 0;
            if (dataGridView1.Rows[rowIndex].Cells[0].Value != null) //is not new row
            {
                _bs._loidtid(siaqodb, (int)dataGridView1.Rows[rowIndex].Cells[0].Value, metaType, fi.Name, ref oids,
                    ref TID);
                if (oids.Count == 0 || TID <= 0)
                {
                }
                else
                {
                    var mtOfComplex = FindMeta(TID);

                    OnOpenObjects(new MetaEventArgs(mtOfComplex, oids));
                }
            }
        }

        private void EditArray(int rowIndex, int columnIndex)
        {
            //generate problems on mac osx
            /*
            object val = this.dataGridView1.Rows[rowIndex].Cells[columnIndex].Value;

            EditArray eaw = new EditArray();
            eaw.SetArrayType(this.dataGridView1.Columns[columnIndex].ValueType);
            if (this.dataGridView1.Columns[columnIndex].ValueType == typeof(byte[]))
            {
                MessageBox.Show("Binary data cannot be edited!");
                return;
            }
            if (val != null && val is Array)
            {

                Array ar = (Array)val;
                eaw.SetArrayValue(ar);

            }
            DialogResult dialog = eaw.ShowDialog();
            if (dialog==DialogResult.OK)
            {
                Array ar = eaw.GetArrayValues();

                try
                {

                    Sqo.Internal._bs._uf(siaqodb, oids[rowIndex], metaType, metaType.Fields[columnIndex - 1].Name, ar);
                    dataGridView1.Rows[rowIndex].Cells[columnIndex].ErrorText = string.Empty;
                }
                catch (SiaqodbException ex)
                {
                    if (ex.Message.StartsWith("Type of value should be:"))
                    {
                        dataGridView1.Rows[rowIndex].Cells[columnIndex].ErrorText = ex.Message;
                    }
                }
                catch (Exception ex)
                {
                    dataGridView1.Rows[rowIndex].Cells[columnIndex].ErrorText = ex.Message;
                }

            }*/
        }

        private MetaType FindMeta(int TID)
        {
            return typesList.First(tii => tii.TypeID == TID);
        }

        private void dataGridView1_UserAddedRow(object sender, DataGridViewRowEventArgs e)
        {
            //on MacOSX this is called without reason and new objects are created
            /*
            int oid = Sqo.Internal._bs._io(siaqodb, metaType);
            this.oids.Add(oid);*/
        }

        private void dataGridView1_UserDeletingRow(object sender, DataGridViewRowCancelEventArgs e)
        {
            if (e.Row.Cells[0].Value is int)
            {
                if (MessageBox.Show("Are you sure to delete this object?", "", MessageBoxButtons.YesNo) ==
                    DialogResult.No)
                {
                    e.Cancel = true;
                }
                else
                {
                    var oid = (int)e.Row.Cells[0].Value;
                    _bs._do(siaqodb, oid, metaType);
                    oids.Remove(oid);
                }
            }
            else //is new
            {
                var oid = oids[oids.Count - 1];
                _bs._do(siaqodb, oid, metaType);
                oids.Remove(oid);
            }
        }

        private void dataGridView1_CellValueNeeded(object sender, DataGridViewCellValueEventArgs e)
        {
            if (dataGridView1.Rows[e.RowIndex].IsNewRow) //new record
            {
            }
            else
            {
                if (e.RowIndex > oids.Count - 1)
                {
                }
                else
                {
                    if (e.ColumnIndex == 0)
                    {
                        e.Value = oids[e.RowIndex];
                    }
                    else
                    {
                        var fi = metaType.Fields[e.ColumnIndex - 1];
                        if (fi.FieldType == null) //complex type
                        {
                            var TID = 0;
                            var isArray = false;
                            _bs._ltid(siaqodb, (int)dataGridView1.Rows[e.RowIndex].Cells[0].Value, metaType, fi.Name,
                                ref TID, ref isArray);
                            if (TID <= 0)
                            {
                                if (TID == -31)
                                    e.Value = "[Dictionary<,>]";
                                else if (TID == -32)
                                    e.Value = "[Jagged Array]";
                                else
                                    e.Value = "[null]";
                            }
                            else
                            {
                                var mtOfComplex = FindMeta(TID);
                                if (isArray)
                                {
                                    var name = mtOfComplex.Name.Split(',');
                                    e.Value = name[0] + " []";
                                }
                                else
                                {
                                    var name = mtOfComplex.Name.Split(',');
                                    e.Value = name[0];
                                }
                            }
                        }
                        else
                        {
                            e.Value = siaqodb.LoadValue(oids[e.RowIndex], metaType.Fields[e.ColumnIndex - 1].Name,
                                metaType);
                        }

                        if (e.Value == null) e.Value = "[null]";
                    }
                }
            }
        }


        private void dataGridView1_CellValueChanged(object sender, DataGridViewCellEventArgs e)
        {
        }

        private void dataGridView1_CellValuePushed(object sender, DataGridViewCellValueEventArgs e)
        {
            //problems on MacOSX, this method is never called
            /*
            if (e.ColumnIndex == 0)
            {
                return;
            }
            try
            {

                Sqo.Internal._bs._uf(siaqodb, oids[e.RowIndex], metaType, metaType.Fields[e.ColumnIndex - 1].Name, e.Value);
                dataGridView1.Rows[e.RowIndex].Cells[e.ColumnIndex].ErrorText = string.Empty;
            }
            catch (SiaqodbException ex)
            {
                if (ex.Message.StartsWith("Type of value should be:"))
                {
                    dataGridView1.Rows[e.RowIndex].Cells[e.ColumnIndex].ErrorText = ex.Message;
                }
            }
            catch (Exception ex)
            {
                dataGridView1.Rows[e.RowIndex].Cells[e.ColumnIndex].ErrorText = ex.Message;
            }*/
        }
    }

    public class MetaEventArgs : EventArgs
    {
        public MetaType mType;
        public List<int> oids;

        public MetaEventArgs(MetaType mType, List<int> oids)
        {
            this.mType = mType;
            this.oids = oids;
        }
    }
}