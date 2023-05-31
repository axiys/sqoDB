using System;
using System.Collections;
using System.IO;
using System.Windows.Forms;

namespace sqoDB.Manager
{
    public partial class UCQuery : UserControl
    {
        private string file;
        private string path;

        public UCQuery()
        {
            InitializeComponent();
        }

        public void Initialize(string path)
        {
            var appPath = Path.GetDirectoryName(Application.ExecutablePath);
        }

        private void button1_Click(object sender, EventArgs e)
        {
        }

        public void Save()
        {
            if (file == null)
            {
                var sfd = new SaveFileDialog();
                sfd.DefaultExt = ".linq";
                sfd.Filter = "(*.linq)|*.linq|All Files(*.*)|*.*";
                var dg = sfd.ShowDialog();
                if (dg == DialogResult.OK)
                    using (var sw = new StreamWriter(sfd.FileName))
                    {
                        sw.Write(textEditorControl1.Text);
                        file = sfd.FileName;
                    }
            }
            else
            {
                using (var sw = new StreamWriter(file))
                {
                    sw.Write(textEditorControl1.Text);
                }
            }
        }

        public void SaveAs()
        {
            var sfd = new SaveFileDialog();
            sfd.DefaultExt = ".linq";
            sfd.Filter = "(*.linq)|*.linq|All Files(*.*)|*.*";
            var dg = sfd.ShowDialog();
            if (dg == DialogResult.OK)
                using (var sw = new StreamWriter(sfd.FileName))
                {
                    sw.Write(textEditorControl1.Text);
                    file = sfd.FileName;
                }
        }


        public void Execute(string path)
        {
            if (this.path != path)
            {
                if (!Directory.Exists(path))
                {
                    textBox1.Text = "Invalid folder! choose a valid database folder";
                    tabControl1.SelectedIndex = 1;
                    return;
                }

                this.path = path;
            }

            textBox1.Text = "";

            SiaqodbConfigurator.EncryptedDatabase = false;

            var siaqodbConfig = new Siaqodb(Application.StartupPath);
            var namespaces = siaqodbConfig.LoadAll<NamespaceItem>();
            var references = siaqodbConfig.LoadAll<ReferenceItem>();
            siaqodbConfig.Close();

            EncryptionSettings.SetEncryptionSettings(); //set back settings

            var ifEncrypted = "";
            if (EncryptionSettings.IsEncryptedChecked)
            {
                ifEncrypted = @" SiaqodbConfigurator.EncryptedDatabase=true;
                                 SiaqodbConfigurator.SetEncryptor(BuildInAlgorithm." + EncryptionSettings.Algorithm +
                              @"); 

                                ";
                if (!string.IsNullOrEmpty(EncryptionSettings.Pwd))
                    ifEncrypted += @"SiaqodbConfigurator.SetEncryptionPassword(" + EncryptionSettings.Pwd + ");";
            }
#if TRIAL
            ifEncrypted += @" SiaqodbConfigurator.SetTrialLicense("""+TrialLicense.LicenseKey+@""");";
#endif
            var metBody = ifEncrypted + @" Siaqodb siaqodb = Sqo.Internal._bs._ofm(@""" + this.path +
                          @""",""SiaqodbManager,SiaqodbManager2"");
			
							object list= (" + textEditorControl1.Text + @").ToList();
                            siaqodb.Close();
                            return list;
							 ";
            var c = new CodeDom();
            //c.AddReference(@"System.Core.dll");
            //c.AddReference(@"siaqodb.dll");
            //c.AddReference(@"System.Windows.Forms.dll");


            foreach (var refi in references) c.AddReference(refi.Item);
            var n = c.AddNamespace("LINQQuery");
            foreach (var nitem in namespaces) n.Imports(nitem.Item);
            n.Imports("System.Collections.Generic")
                .Imports("System.Linq")
                .Imports("Sqo")
                .AddClass(
                    c.Class("RunQuery")
                        .AddMethod(c.Method("object", "FilterByLINQ", "", metBody)));

            var assembly = c.Compile(WriteErrors);
            if (assembly != null)
            {
                var t = assembly.GetType("LINQQuery.RunQuery");
                var method = t.GetMethod("FilterByLINQ");

                try
                {
                    var retVal = method.Invoke(null, null);
                    //Type[] tt = retVal.GetType().GetGenericArguments();
                    var w = (IList)retVal;
                    //ArrayList ar = new ArrayList();
                    //while (w.MoveNext())
                    //{
                    //    ar.Add(w.Current);

                    //}
                    dataGridView1.DataSource = w;
                    dataGridView1.AutoGenerateColumns = true;
                    tabControl1.SelectedIndex = 0;
                    //this.lblNrRows.Text = ar.Count + " rows";
                }
                catch (Exception ex)
                {
                    WriteErrors(ex.ToString());
                    tabControl1.SelectedIndex = 1;
                }
            }
            else
            {
                tabControl1.SelectedIndex = 1;
            }
        }

        private void WriteErrors(string errorLine)
        {
            textBox1.Text += errorLine + "\r\n";
        }

        public string GetFile()
        {
            return file;
        }

        internal void SetText(string s, string file)
        {
            textEditorControl1.Text = s;
            this.file = file;
        }
    }
}