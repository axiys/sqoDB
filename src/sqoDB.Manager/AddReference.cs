using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Windows.Forms;
using sqoDB.Attributes;

namespace sqoDB.Manager
{
    public partial class AddReference : Form
    {
        private readonly List<ReferenceItem> assemblies = new List<ReferenceItem>();
        private readonly List<NamespaceItem> namespaces = new List<NamespaceItem>();

        public AddReference()
        {
            InitializeComponent();
        }

        private void btnOK_Click(object sender, EventArgs e)
        {
            if (Directory.Exists(Application.StartupPath))
            {
                assemblies.Clear();
                namespaces.Clear();
                var siaqodb = new Siaqodb(Application.StartupPath);
                try
                {
                    siaqodb.DropType<ReferenceItem>();
                    siaqodb.DropType<NamespaceItem>();
                    foreach (var o in listBox1.Items)
                    {
                        var refItem = o as ReferenceItem;
                        if (refItem == null) refItem = new ReferenceItem(o.ToString());
                        assemblies.Add(refItem);
                        siaqodb.StoreObject(refItem);

                        if (File.Exists(refItem.Item))
                            try
                            {
                                File.Copy(refItem.Item,
                                    Application.StartupPath + Path.DirectorySeparatorChar +
                                    Path.GetFileName(refItem.Item), true);
                            }
                            catch
                            {
                            }
                    }

                    foreach (var s in textBox1.Text.Split(new[] { Environment.NewLine },
                                 StringSplitOptions.RemoveEmptyEntries))
                    {
                        var nobj = new NamespaceItem(s);
                        namespaces.Add(nobj);
                        siaqodb.StoreObject(nobj);
                    }
                }
                finally
                {
                    siaqodb.Close();
                }
            }

            DialogResult = DialogResult.OK;
        }

        public List<ReferenceItem> GetReferences()
        {
            return assemblies;
        }

        public List<NamespaceItem> GetNamespaces()
        {
            return namespaces;
        }

        private void btnCancel_Click(object sender, EventArgs e)
        {
            Close();
        }

        private void btnAddReference_Click(object sender, EventArgs e)
        {
            var opf = new OpenFileDialog();
            opf.Filter = "assembly files (*.dll;*.exe)|*.dll;*.exe";
            opf.InitialDirectory = Application.StartupPath;
            opf.Multiselect = false;
            if (opf.ShowDialog() == DialogResult.OK) listBox1.Items.Add(opf.FileName);
        }

        private void btnRemoveReference_Click(object sender, EventArgs e)
        {
            if (listBox1.SelectedItem != null) listBox1.Items.Remove(listBox1.SelectedItem);
        }

        private void AddReference_Load(object sender, EventArgs e)
        {
            if (Directory.Exists(Application.StartupPath))
            {
                var siaqodb = new Siaqodb(Application.StartupPath);
                try
                {
                    var references = siaqodb.LoadAll<ReferenceItem>();
                    foreach (var refItem in references) listBox1.Items.Add(refItem);
                    var namespacesItems = siaqodb.LoadAll<NamespaceItem>();
                    foreach (var nItem in namespacesItems) textBox1.Text += nItem + Environment.NewLine;
                }
                finally
                {
                    siaqodb.Close();
                }
            }
        }

        private void btnAddDefault_Click(object sender, EventArgs e)
        {
            listBox1.Items.Add("System.dll");
            listBox1.Items.Add("System.Core.dll");
            listBox1.Items.Add("System.Windows.Forms.dll");
            listBox1.Items.Add("siaqodb.dll");
        }
    }

    [Obfuscation(Exclude = true)]
    public class ReferenceItem : SqoDataObject
    {
        [MaxLength(2000)] public string Item;

        public ReferenceItem()
        {
        }

        public ReferenceItem(string item)
        {
            Item = item;
        }

        public override string ToString()
        {
            return Item;
        }
    }

    [Obfuscation(Exclude = true)]
    public class NamespaceItem : SqoDataObject
    {
        [MaxLength(2000)] public string Item;

        public NamespaceItem()
        {
        }

        public NamespaceItem(string item)
        {
            Item = item;
        }

        public override string ToString()
        {
            return Item;
        }
    }
}