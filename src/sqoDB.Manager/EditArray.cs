using System;
using System.Windows.Forms;

namespace sqoDB.Manager
{
    public partial class EditArray : Form
    {
        private Type elementType;
        private Array values;

        public EditArray()
        {
            InitializeComponent();
        }

        public void SetArrayValue(Array arr)
        {
            foreach (var obj in arr)
                if (textBox1.Text == string.Empty)
                    textBox1.AppendText(obj.ToString());
                else
                    textBox1.AppendText(Environment.NewLine + obj);
        }

        public Array GetArrayValues()
        {
            return values;
        }

        private void btnSave_Click(object sender, EventArgs e)
        {
            if (textBox1.Text.Trim() != string.Empty)
                try
                {
                    var arrayStr = textBox1.Text.Split(new[] { Environment.NewLine },
                        StringSplitOptions.RemoveEmptyEntries);
                    values = Array.CreateInstance(elementType, arrayStr.Length);
                    for (var i = 0; i < arrayStr.Length; i++)
                        values.SetValue(Convert.ChangeType(arrayStr[i], elementType), i);
                }
                catch (Exception ex)
                {
                    MessageBox.Show(ex.Message);
                    return;
                }
            else
                values = Array.CreateInstance(elementType, 0);

            DialogResult = DialogResult.OK;
        }

        private void btnCancel_Click(object sender, EventArgs e)
        {
            DialogResult = DialogResult.Cancel;
        }

        internal void SetArrayType(Type type)
        {
            elementType = type.GetElementType();
        }
    }
}