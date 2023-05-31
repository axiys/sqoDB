using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Windows.Forms;
using sqoDB.Attributes;
using sqoDB.Internal;

namespace sqoDB.Manager
{
    public partial class Main : Form
    {
        private Siaqodb siaqodb;
        private List<MetaType> siaqodbList;

        public Main()
        {
            InitializeComponent();
        }

        private void treeView1_AfterSelect(object sender, TreeViewEventArgs e)
        {
        }

        private void tabPage1_Click(object sender, EventArgs e)
        {
        }

        private void aboutToolStripMenuItem1_Click(object sender, EventArgs e)
        {
            //AboutBox1 ab = new AboutBox1();
            //ab.ShowDialog();
        }

        private void toolStripButton1_Click(object sender, EventArgs e)
        {
            //GC.Collect();

            if (Directory.Exists(cmbDBPath.Text))
            {
                if (!cmbDBPath.Items.Contains(cmbDBPath.Text))
                {
                    cmbDBPath.ComboBox.Items.Add(cmbDBPath.Text);
                    SiaqodbConfigurator.EncryptedDatabase = false;

                    var siaqodbConfig = new Siaqodb(Application.StartupPath);

                    siaqodbConfig.StoreObject(new ConnectionItem(cmbDBPath.Text));
                    siaqodbConfig.Close();
                    EncryptionSettings.SetEncryptionSettings(); //set back settings
                }

                siaqodb = _bs._b(cmbDBPath.Text);

                siaqodbList = siaqodb.GetAllTypes();
                treeView1.Nodes.Clear();
                foreach (var mt in siaqodbList)
                {
                    var nodeType = new TreeNode(mt.Name);
                    nodeType.Tag = mt;
                    nodeType.ImageIndex = 0;
                    nodeType.SelectedImageIndex = 0;
                    nodeType.ContextMenuStrip = contextMenuStrip1;
                    treeView1.Nodes.Add(nodeType);
                    foreach (var mf in mt.Fields)
                    {
                        var nodeField = new TreeNode();
                        if (mf.FieldType != null)
                            nodeField.Text = mf.Name + "(" + mf.FieldType + ")";
                        else
                            nodeField.Text = mf.Name + "(ComplexType)";
                        nodeField.ImageIndex = 1;
                        nodeField.SelectedImageIndex = 1;
                        nodeType.Nodes.Add(nodeField);
                    }
                }
            }
            else
            {
                MessageBox.Show("Select a valid folder path!");
            }
        }


        private void Main_Load(object sender, EventArgs e)
        {
        }

        private void newQueryToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var uq = new UCQuery();
            uq.Initialize(cmbDBPath.Text);
            uq.Dock = DockStyle.Fill;
            var tab = new TabPage("NewQuery1");
            tab.Controls.Add(uq);
            tabControl1.TabPages.Add(tab);
            tabControl1.SelectedTab = tab;
            executeToolStripMenuItem.Enabled = true;
            btnExecuteToolbar.Enabled = true;
        }

        private void treeView1_DoubleClick(object sender, EventArgs e)
        {
            LoadObjects();
        }

        private void LoadObjects()
        {
            var mt = treeView1.SelectedNode.Tag as MetaType;
            if (mt != null)
            {
                var uco = new UCObjects();
                uco.Initialize(mt, siaqodb, siaqodbList);
                uco.OpenObjects += uco_OpenObjects;
                uco.Dock = DockStyle.Fill;
                var tab = new TabPage(mt.Name);
                tab.Controls.Add(uco);
                tabControl1.TabPages.Add(tab);
                tabControl1.SelectedTab = tab;
                uco.Refresh();
            }
        }

        private void uco_OpenObjects(object sender, MetaEventArgs e)
        {
            var uco = new UCObjects();
            uco.Initialize(e.mType, siaqodb, siaqodbList, e.oids);
            uco.OpenObjects += uco_OpenObjects;
            uco.Dock = DockStyle.Fill;
            var tab = new TabPage(e.mType.Name);
            tab.Controls.Add(uco);
            tabControl1.TabPages.Add(tab);
            tabControl1.SelectedTab = tab;
            uco.Refresh();
        }

        private void btnCloseTab_Click(object sender, EventArgs e)
        {
        }

        private void btnOpenFolder_Click(object sender, EventArgs e)
        {
            var fb = new FolderBrowserDialog();
            fb.SelectedPath = Environment.CurrentDirectory;
            if (fb.ShowDialog() == DialogResult.OK) cmbDBPath.Text = fb.SelectedPath;
        }

        private void btnNewLinqEditor_Click(object sender, EventArgs e)
        {
            newQueryToolStripMenuItem_Click(sender, e);
        }

        private void referencesToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var adref = new AddReference();
            if (adref.ShowDialog() == DialogResult.OK)
            {
            }
        }


        private void executeToolStripMenuItem_Click(object sender, EventArgs e)
        {
            if (tabControl1.SelectedTab != null)
            {
                var query = tabControl1.SelectedTab.Controls[0] as UCQuery;
                if (query != null) query.Execute(cmbDBPath.Text);
            }
        }

        private void tabControl1_SelectedIndexChanged(object sender, EventArgs e)
        {
            if (tabControl1.SelectedTab != null)
            {
                var query = tabControl1.SelectedTab.Controls[0] as UCQuery;
                if (query != null)
                {
                    //query.Execute();
                    executeToolStripMenuItem.Enabled = true;
                    btnExecuteToolbar.Enabled = true;
                }
                else
                {
                    executeToolStripMenuItem.Enabled = false;
                    btnExecuteToolbar.Enabled = false;
                }

                tabControl1.SelectedTab.ToolTipText = tabControl1.SelectedTab.Text;
            }
        }

        private void btnExecuteToolbar_Click(object sender, EventArgs e)
        {
            executeToolStripMenuItem_Click(sender, e);
        }

        private void saveToolStripMenuItem_Click(object sender, EventArgs e)
        {
            if (tabControl1.SelectedTab != null)
            {
                var query = tabControl1.SelectedTab.Controls[0] as UCQuery;
                if (query != null)
                {
                    query.Save();
                    var fileName = query.GetFile();
                    if (fileName != null)
                    {
                        var fname = Path.GetFileName(fileName);
                        tabControl1.SelectedTab.Text = fname;
                    }
                }
            }
        }

        private void btnSaveToolbar_Click(object sender, EventArgs e)
        {
            saveToolStripMenuItem_Click(sender, e);
        }

        private void saveAsToolStripMenuItem_Click(object sender, EventArgs e)
        {
            if (tabControl1.SelectedTab != null)
            {
                var query = tabControl1.SelectedTab.Controls[0] as UCQuery;
                if (query != null)
                {
                    query.SaveAs();
                    var fileName = query.GetFile();
                    if (fileName != null)
                    {
                        var fname = Path.GetFileName(fileName);
                        tabControl1.SelectedTab.Text = fname;
                    }
                }
            }
        }

        private void exitToolStripMenuItem_Click(object sender, EventArgs e)
        {
            Application.Exit();
        }

        private void openLINQToolStrip_Click(object sender, EventArgs e)
        {
            var opf = new OpenFileDialog();
            opf.Filter = "(*.linq)|*.linq|All Files(*.*)|*.*";
            opf.InitialDirectory = Environment.CurrentDirectory;
            if (opf.ShowDialog() == DialogResult.OK)
                using (var sr = new StreamReader(opf.FileName))
                {
                    var s = sr.ReadToEnd();
                    var uq = new UCQuery();
                    uq.Initialize(cmbDBPath.Text);
                    uq.Dock = DockStyle.Fill;
                    var tab = new TabPage(Path.GetFileName(opf.FileName));
                    tab.Controls.Add(uq);
                    tabControl1.TabPages.Add(tab);
                    tabControl1.SelectedTab = tab;
                    uq.SetText(s, opf.FileName);
                    executeToolStripMenuItem.Enabled = true;
                    btnExecuteToolbar.Enabled = true;
                }
        }

        private void btnOpenToolbar_Click(object sender, EventArgs e)
        {
            openLINQToolStrip_Click(sender, e);
        }

        private void loadObjectsToolStripMenuItem_Click(object sender, EventArgs e)
        {
            treeView1_DoubleClick(sender, e);
        }

        private void lnkRunDemo_LinkClicked(object sender, LinkLabelLinkClickedEventArgs e)
        {
            try
            {
                Process.Start(Application.StartupPath + Path.DirectorySeparatorChar + "demo" +
                              Path.DirectorySeparatorChar + "SiaqodbManager.mp4");
            }
            catch (Exception ex)
            {
                MessageBox.Show("Cannot start DEMO!");
            }
        }

        private void lnkForum_LinkClicked(object sender, LinkLabelLinkClickedEventArgs e)
        {
            try
            {
                Process.Start("http://forum.siaqodb.com");
            }
            catch (Exception ex)
            {
            }
        }

        private void lnkBlog_LinkClicked(object sender, LinkLabelLinkClickedEventArgs e)
        {
            try
            {
                Process.Start("http://siaqodb.com/?page_id=13");
            }
            catch (Exception ex)
            {
            }
        }

        private void lnkSendEmail_LinkClicked(object sender, LinkLabelLinkClickedEventArgs e)
        {
            try
            {
                Process.Start("mailto:support@siaqodb.com");
            }
            catch (Exception ex)
            {
            }
        }

        private void btnCloseTabb_Click(object sender, EventArgs e)
        {
            if (tabControl1.TabPages.Count > 0)
                if (tabControl1.SelectedTab != null)
                {
                    foreach (Control c in tabControl1.SelectedTab.Controls) c.Dispose();
                    tabControl1.TabPages.Remove(tabControl1.SelectedTab);
                }
        }

        private void encryptionToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var es = new EncryptionSettings();

            if (es.ShowDialog() == DialogResult.OK)
            {
                if (tabControl1.TabPages.Count > 0)
                {
                    var tabs = new List<TabPage>();
                    foreach (TabPage tab in tabControl1.TabPages) tabs.Add(tab);
                    foreach (var tab in tabs)
                    {
                        foreach (Control c in tab.Controls) c.Dispose();
                        tabControl1.TabPages.Remove(tab);
                    }
                }

                treeView1.Nodes.Clear();
            }
        }

        private void helpToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var path = Application.StartupPath + Path.DirectorySeparatorChar + "Help" + Path.DirectorySeparatorChar +
                       "Help.rtf";
            try
            {
                Process.Start(path);
            }
            catch (Exception ex)
            {
                MessageBox.Show("Cannot open Help.rtf");
            }
        }

        private void Main_Shown(object sender, EventArgs e)
        {
#if TRIAL
            string folder = Application.StartupPath;
            string trialFile = folder + System.IO.Path.DirectorySeparatorChar + "trial.lic";
            if (System.IO.File.Exists(trialFile))
            {
                string text = System.IO.File.ReadAllText(trialFile);
                try
                {

                    SiaqodbConfigurator.SetTrialLicense(text);
                    Siaqodb siaqodbConfigTemp = new Siaqodb(Application.StartupPath);
                    siaqodbConfigTemp.Close();
                    TrialLicense.LicenseKey = text;
                }
                catch (Sqo.Exceptions.InvalidLicenseException ex)
                {
                    MessageBox.Show(ex.Message);
                    this.Close();
                    return;
                }
                catch (Exception ex)
                {
                    MessageBox.Show(ex.Message);
                    TrialLicenseFrm trialWnd = new TrialLicenseFrm();
                    if (trialWnd.ShowDialog() == DialogResult.OK)
                    {
                        string trialKey = trialWnd.GetLicenseKey();
                        System.IO.File.WriteAllText(trialFile, trialKey);
                    }
                    else
                    {
                        this.Close();
                        return;
                    }
                }
            }
            else
            {
                TrialLicenseFrm trialWnd = new TrialLicenseFrm();
                if (trialWnd.ShowDialog() == DialogResult.OK)
                {
                    string trialKey = trialWnd.GetLicenseKey();
                    System.IO.File.WriteAllText(trialFile, trialKey);
                }
                else
                {
                    this.Close();
                    return;
                }
            }
#endif

            var siaqodbConfig = new Siaqodb(Application.StartupPath);
            //siaqodbConfig.DropType<ConnectionItem>();
            var list = siaqodbConfig.LoadAll<ConnectionItem>();

            foreach (var item in list) cmbDBPath.ComboBox.Items.Add(item.Item);
            siaqodbConfig.Close();

            var siaqodbRef = new Siaqodb(Application.StartupPath);

            var references = siaqodbRef.LoadAll<ReferenceItem>();
            foreach (var refi in references)
                if (File.Exists(refi.Item))
                    try
                    {
                        File.Copy(refi.Item,
                            Application.StartupPath + Path.DirectorySeparatorChar + Path.GetFileName(refi.Item), true);
                    }
                    catch
                    {
                    }

            siaqodbRef.Close();
        }
    }

    [Obfuscation(Exclude = true)]
    public class ConnectionItem : SqoDataObject
    {
        [MaxLength(2000)] public string Item;

        public ConnectionItem(string item)
        {
            Item = item;
        }

        public ConnectionItem()
        {
        }

        public override string ToString()
        {
            return Item;
        }
    }
}