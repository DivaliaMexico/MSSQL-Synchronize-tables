using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;

namespace Sync
{
    class Program
    {
        private static void Main(string[] args)
        {
            var MyIni = new IniFile("Settings.ini");
            var VariableSource = MyIni.Read("ConnString", "SourceServer");
            var VariableRemote = MyIni.Read("ConnString", "RemoteServer");

            if (args.Length == 0)
            {
                Console.WriteLine("Divalia S.A de C.V - Table Mirroring Tool v0.10");
                Console.WriteLine("https://www.divalia.mx");
                Console.WriteLine("Usage: Sync.exe TableName");
                Console.ReadLine();
            }
            else
            {
                string connectionString1 = VariableSource.ToString();
                string connectionString2 = VariableRemote.ToString();

                int num1 = 50;
                int num2 = 50;

                SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(connectionString2, SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.KeepNulls);
                SqlConnection sqlConnection = new SqlConnection(connectionString1);
                SqlConnection connection = new SqlConnection(connectionString2);
                
                sqlConnection.Open();
                connection.Open();

                using (SqlCommand sqlCommand = new SqlCommand(string.Format("TRUNCATE TABLE {0}", (object)args[0]), connection))
                {
                    Console.WriteLine("Divalia S.A de C.V - Table Mirroring Tool v0.10");
                    Console.WriteLine("https://www.divalia.mx");
                    Console.WriteLine("Truncating Table");
                    sqlCommand.ExecuteNonQuery();
                    connection.Close();
                }
                SqlCommand selectCommand = new SqlCommand(string.Format("select * from sysobjects where name='{0}'", (object)args[0]));
                selectCommand.Connection = sqlConnection;
                selectCommand.CommandType = CommandType.Text;
                DataTable dataTable = new DataTable();
                new SqlDataAdapter(selectCommand).Fill(dataTable);
                int num3 = 0;
                int count = dataTable.Rows.Count;
                Console.WriteLine("Found {0} Tables to copy.", (object)count);
                Console.WriteLine("");
                sqlBulkCopy.SqlRowsCopied += new SqlRowsCopiedEventHandler(Program.c_SqlRowsCopied);
                foreach (DataRow dataRow in (InternalDataCollectionBase)dataTable.Rows)
                {
                    Console.WriteLine("Coping table {0} of {1}: {2}", (object)++num3, (object)count, dataRow["name"]);
                    selectCommand.CommandText = string.Format("select * from [{0}]", dataRow["name"]);
                    SqlDataReader sqlDataReader = selectCommand.ExecuteReader();
                    sqlBulkCopy.BatchSize = num1;
                    sqlBulkCopy.DestinationTableName = dataRow["name"].ToString();
                    sqlBulkCopy.NotifyAfter = num2;
                    try
                    {
                        sqlBulkCopy.WriteToServer((IDataReader)sqlDataReader);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                    sqlDataReader.Close();
                }
                sqlConnection.Close();
                sqlBulkCopy.Close();
                Console.WriteLine("");
                Console.WriteLine("Done with Sync.");
                Environment.Exit(0);
            }
        }

        private static void c_SqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
        {
            Console.WriteLine(" -- copied {0} rows ...", (object)e.RowsCopied);
        }
    }
}
