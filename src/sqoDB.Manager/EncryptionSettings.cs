using System;
using System.Windows.Forms;

namespace sqoDB.Manager
{
    public partial class EncryptionSettings : Form
    {
        public EncryptionSettings()
        {
            InitializeComponent();
        }

        public static bool IsEncryptedChecked { get; set; }
        public static string Algorithm { get; set; }
        public static string Pwd { get; set; }

        public static void SetEncryptionSettings()
        {
            SiaqodbConfigurator.EncryptedDatabase = IsEncryptedChecked;
            if (SiaqodbConfigurator.EncryptedDatabase)
            {
                SiaqodbConfigurator.SetEncryptor(Algorithm == "AES" ? BuildInAlgorithm.AES : BuildInAlgorithm.XTEA);

                if (!string.IsNullOrEmpty(Pwd)) SiaqodbConfigurator.SetEncryptionPassword(Pwd);
            }
        }

        private void EncryptionSettings_Load(object sender, EventArgs e)
        {
            checkBox1.Checked = IsEncryptedChecked;
            textBox1.Text = Pwd;
            cmbAlgo.Text = string.IsNullOrEmpty(Algorithm) ? "AES" : Algorithm;
        }

        private void checkBox1_CheckedChanged(object sender, EventArgs e)
        {
            textBox1.Enabled = checkBox1.Checked;
            cmbAlgo.Enabled = checkBox1.Checked;
        }

        private void btnOK_Click(object sender, EventArgs e)
        {
            if (MessageBox.Show("Changing encryption settings will disconnect current database,continue?", "Continue",
                    MessageBoxButtons.YesNo) == DialogResult.Yes)
            {
                IsEncryptedChecked = checkBox1.Checked;
                Algorithm = cmbAlgo.Text;
                Pwd = textBox1.Text;

                SetEncryptionSettings();

                DialogResult = DialogResult.OK;

                Close();
            }
        }
    }
}