using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Text;

namespace ImportFanData {
    class Program {
        static void Main(string[] args) {
            Program p = new Program();
            if (p.isDatabaseExists(args[0], "FanDatabase")) {
                if (!p.isDatatableExists(args[0], "RawData")) {
                    p.CreateDatatable(args[0]);
                }
            } else {
                p.CreateDatabase(args[0]);
                p.CreateDatatable(args[0]);
            }
            p.ImportData(args[0], args[1]);
        }
        public bool isDatabaseExists(string Server, string Database) {
            string connStr = "Server=" + Server + ";Integrated security=SSPI;";
            string sql = "SELECT * FROM master.dbo.sysdatabases WHERE name ='" + Database + "'";
            bool isExist = false;
            using (SqlConnection conn = new SqlConnection(connStr)) {
                conn.Open();
                using (SqlCommand cmd = new SqlCommand(sql, conn)) {
                    using (SqlDataReader reader = cmd.ExecuteReader()) {
                        isExist = reader.HasRows;
                    }
                }
                conn.Close();
            }
            return isExist;
        }
        public bool isDatatableExists(string Server, string Datatable) {
            string connStr = "Server=" + Server + ";Integrated security=SSPI;Initial Catalog=FanDatabase;";
            string sql = "select case when exists((select * from information_schema.tables where table_name = '" + Datatable + "')) then 1 else 0 end";
            bool isExist = false;

            using (SqlConnection conn = new SqlConnection(connStr)) {
                conn.Open();
                using (SqlCommand cmd = new SqlCommand(sql, conn)) {
                    isExist = (Convert.ToInt32(cmd.ExecuteScalar()) == 1) ? true : false;
                }
                conn.Close();
            }
            return isExist;
        }
        public void CreateDatabase(string Server) {
            string str1;
            SqlConnection conn = new SqlConnection("Server=" + Server + ";Integrated security=SSPI;");

            str1 = "CREATE DATABASE FanDatabase ON PRIMARY " +
             "(NAME = FanDatabase, " +
             "FILENAME = 'D:\\FanDatabase.mdf', " +
             "SIZE = 8MB, " +
             "FILEGROWTH = 64MB)" +
             "LOG ON " +
             "(NAME = FanDatabase_Log, " +
             "FILENAME = 'D:\\FanDatabaseLog.ldf', " +
             "SIZE = 8MB, " +
             "FILEGROWTH = 64MB)";

            SqlCommand cmd = new SqlCommand(str1, conn);
            try {
                conn.Open();
                cmd.ExecuteNonQuery();
            }
            catch {
            }
            finally {
                if (conn.State == ConnectionState.Open) {
                    conn.Close();
                }
            }
        }
        public void CreateDatatable(string Server) {
            string sql;
            SqlConnection conn = new SqlConnection("Server=" + Server + ";Initial Catalog=FanDatabase;Integrated Security=SSPI;");

            sql = "[N] decimal(14,6), " +
              "[Ps] decimal(14,6), " +
              "[Q] decimal(14,6), " +
              "[FanType] nvarchar(250), " +
              "[P_unit] nvarchar(20), " +
              "[Q_unit] nvarchar(20), " +
              "[FileName] nvarchar(100), " +
              "[state_rec] Int, " +
              "[ImportDate] datetime, " +
              "[FileSource] nvarchar(100) " +
              ");";

            SqlCommand cmd1 = new SqlCommand("create table RawData(" + sql, conn);
            SqlCommand cmd2 = new SqlCommand("create table MaxData(" + sql, conn);
            try {
                conn.Open();
                cmd1.ExecuteNonQuery();
                cmd2.ExecuteNonQuery();
            }
            catch {
            }
            finally {
                if (conn.State == ConnectionState.Open) {
                    conn.Close();
                }
            }
        }
        public DataTable CSVtoDT(string FilePath) {
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            string strTableHead = "N,Ps,Q,FanType,P_unit,Q_unit,FileName,state_rec,ImportDate,FileSource";

            string[] tableHead = strTableHead.Split(',');
            int colCount = tableHead.Length;

            DataTable DT1 = new DataTable();
            try {
                FileStream fs1 = new FileStream(FilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read);
                StreamReader strReader1 = new StreamReader(fs1, Encoding.GetEncoding(950));
                string strLine1; //data (each feature)
                int headCount = 0;
                bool isFirst = true;
                int rowCount = 0;
                string[] arrayLine;
                string[] featureLine = new string[10];

                for (int c = 0; c < colCount; c++) {
                    strLine1 = strReader1.ReadLine();
                    if (strLine1 != null) {
                        arrayLine = strLine1.Split(',');
                        rowCount = arrayLine.Length;

                        if (isFirst) {
                            for (int i = 0; i < rowCount + 1; i++) {
                                DataColumn dtColumn = new DataColumn();
                                DT1.Columns.Add(dtColumn);
                            }
                            isFirst = false;
                        }
                        DataRow dtRow = DT1.NewRow();
                        for (int j = 0; j < rowCount + 1; j++) {
                            List<string> listData = new List<string>(arrayLine);
                            listData.Insert(0, tableHead[headCount]);
                            string[] strData = listData.ToArray();

                            dtRow[j] = strData[j].Replace("\"", "");
                        }
                        headCount++;
                        DT1.Rows.Add(dtRow);
                    }
                }
                strReader1.Close();
                fs1.Close();
            }
            catch {
            }
            return DT1;
        }
        public DataTable dtTranspose(DataTable DT1) {
            DataTable dtNew = new DataTable();
            for (int i = 0; i < DT1.Rows.Count; i++) {
                DataColumn dtColumn = new DataColumn();
                dtNew.Columns.Add(dtColumn);
                dtNew.Columns[i].ColumnName = DT1.Rows[i][0].ToString();
            }
            for (int j = 0; j < DT1.Columns.Count - 1; j++) {
                DataRow dtRow = dtNew.NewRow();
                for (int k = 0; k < DT1.Rows.Count; k++) {
                    dtRow[k] = DT1.Rows[k][j + 1];
                }
                dtNew.Rows.Add(dtRow);
            }
            return dtNew;
        }
        public void ImportData(string Server, string FolderPath) {
            /****************connection*****************/
            SqlConnection conn_fan = new SqlConnection("Data Source=" + Server + ";Initial Catalog=FanDatabase;Integrated Security=SSPI;");
            conn_fan.Open();

            SqlCommand cmd = new SqlCommand();
            cmd.Connection = conn_fan;
            /******************************************/
            string sql1;
            int state_rec = 1; //2:feature; 1:raw data(normal); 0:not normal data or be removed

            try {
                foreach (string strFile in System.IO.Directory.GetFiles(FolderPath)) {
                    DataTable DT_fan = CSVtoDT(strFile);
                    DT_fan = dtTranspose(DT_fan);
                    sql1 = "";
                    DateTime timeT = DateTime.Now;
                    #region RawData
                    for (int i = 0; i < DT_fan.Rows.Count; i++) {
                        state_rec = (i < 2) ? 2 : 1;
                        DataRow row1 = DT_fan.Rows[i];
                        sql1 = "Insert into RawData([N],[Ps],[Q],[FanType],[P_unit],[Q_unit],[FileName],[state_rec],[ImportDate],[FileSource]) " +
                          "values" +
                          "(@N, @Ps, @Q, @FanType, @P_unit, @Q_unit, @FileName, @state_rec, @ImportDate, @FileSource);";

                        cmd.Parameters.Clear();
                        cmd.CommandText = sql1;

                        cmd.Parameters.Add("@N", SqlDbType.Decimal, 14).Value = strToDecimal(row1["N"].ToString());
                        cmd.Parameters.Add("@Ps", SqlDbType.Decimal, 14).Value = strToDecimal(row1["Ps"].ToString());
                        cmd.Parameters.Add("@Q", SqlDbType.Decimal, 14).Value = strToDecimal(row1["Q"].ToString());
                        cmd.Parameters.Add("@FanType", SqlDbType.NVarChar, 250).Value = row1["FanType"].ToString();
                        cmd.Parameters.Add("@P_unit", SqlDbType.NVarChar, 20).Value = row1["P_unit"].ToString();
                        cmd.Parameters.Add("@Q_unit", SqlDbType.NVarChar, 20).Value = row1["Q_unit"].ToString();
                        cmd.Parameters.Add("@FileName", SqlDbType.NVarChar, 100).Value = row1["FileName"].ToString();
                        cmd.Parameters.Add("@state_rec", SqlDbType.Int, 0).Value = state_rec;
                        cmd.Parameters.Add("@ImportDate", SqlDbType.DateTime, 0).Value = timeT;
                        cmd.Parameters.Add("@FileSource", SqlDbType.NVarChar, 100).Value = strFile;

                        cmd.ExecuteNonQuery();
                    }
                    #endregion

                    #region MaxData
                    /****************select max nt*****************/
                    DataTable DT_fan_MAX = new DataTable();
                    DataRow[] row_MAX = DT_fan.Select("N=(max(N))");

                    DT_fan_MAX = DT_fan.Clone();
                    foreach (var row in row_MAX) {
                        DT_fan_MAX.ImportRow(row);
                    }

                    for (int i = 0; i < DT_fan_MAX.Rows.Count; i++) {
                        DataRow row1 = DT_fan_MAX.Rows[i];
                        sql1 = "Insert into MaxData([N],[Ps],[Q],[FanType],[P_unit],[Q_unit],[FileName],[state_rec],[ImportDate],[FileSource]) " +
                          "values" +
                          "(@N, @Ps, @Q, @FanType, @P_unit, @Q_unit, @FileName, @state_rec, @ImportDate, @FileSource)";

                        cmd.Parameters.Clear();
                        cmd.CommandText = sql1;

                        cmd.Parameters.Add("@N", SqlDbType.Decimal, 14).Value = strToDecimal(row1["N"].ToString());
                        cmd.Parameters.Add("@Ps", SqlDbType.Decimal, 14).Value = strToDecimal(row1["Ps"].ToString());
                        cmd.Parameters.Add("@Q", SqlDbType.Decimal, 14).Value = strToDecimal(row1["Q"].ToString());
                        cmd.Parameters.Add("@FanType", SqlDbType.NVarChar, 250).Value = row1["FanType"].ToString();
                        cmd.Parameters.Add("@P_unit", SqlDbType.NVarChar, 20).Value = row1["P_unit"].ToString();
                        cmd.Parameters.Add("@Q_unit", SqlDbType.NVarChar, 20).Value = row1["Q_unit"].ToString();
                        cmd.Parameters.Add("@FileName", SqlDbType.NVarChar, 100).Value = row1["FileName"].ToString();
                        cmd.Parameters.Add("@state_rec", SqlDbType.Int, 0).Value = state_rec;
                        cmd.Parameters.Add("@ImportDate", SqlDbType.DateTime, 0).Value = timeT;
                        cmd.Parameters.Add("@FileSource", SqlDbType.NVarChar, 100).Value = strFile;

                        cmd.ExecuteNonQuery();
                    }
                    #endregion
                }
            }
            catch {
            }
            finally {
                cmd.Dispose();
                conn_fan.Dispose();
            }
        }
        public object strToDecimal(string s) {
            try {
                decimal decValue = decimal.Parse(s);
                return decValue;
            }
            catch {
                return DBNull.Value;
            }
        }
        public object strToInt(string s) {
            try {
                int intValue = int.Parse(s);
                return int.Parse(s);
            }
            catch {
                return DBNull.Value;
            }
        }
    }
}
