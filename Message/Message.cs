using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using IBM.Data.DB2.iSeries;
using System.Data;
using ETLCountryPack;

namespace Message
{
    public class Message
    {
        //Modificado por jl para la insercion en hd cuando no haya registros en rq para xa
        public Message(string input, string OutputRQ, string OutputHD)
        {
            
            string conStr = ConnectionData.StringConnectionERP.Replace("QGPL", "QGPL,"+ ConnectionData.SchemaDefault);
            Console.WriteLine(conStr);
            //"DataSource=192.168.17.168; Initial Catalog=S2150caw; UserID=MXAPLUS; Password=WN7R3K; Naming=System; LibraryList=QGPL;";
            try
            {
                
                iDB2Connection myConnection = new iDB2Connection(conStr);               
                //"UPDATE MXEIRQ SET  V9ERRD = 'Error Gaspar 1218' WHERE V9CONO = '01' AND V9SERIE = 'A' AND V9FOLIO = '000196' ";

                //Modificado por jl para la insercion en hd cuando no haya registros en rq para xa
                string myExecuteQuery = input;
                Console.WriteLine(input);
                iDB2Command myCommand = new iDB2Command(myExecuteQuery, myConnection);
                myCommand.CommandTimeout = 9000000;
                myCommand.Connection.Open();
                int registros = (int)myCommand.ExecuteNonQuery();
                myConnection.Close();
                if (GlobalStrings.Config == "0")
                {
                    if (registros == 1)
                    {
                        Console.WriteLine("Actualizando UUID en RQ -Message");
                        Console.WriteLine(OutputRQ);
                       myExecuteQuery = OutputRQ;

                    }
                    else
                    {
                        Console.WriteLine("Actualizando UUID en HD -Message");
                        Console.WriteLine(OutputHD);
                        myExecuteQuery = OutputHD;

                    }
                }
                else
                {                  
                        Console.WriteLine("Actualizando UUID en HD -Message");
                    Console.WriteLine(OutputHD);
                    myExecuteQuery = OutputHD;                   
                }

                    myCommand = new iDB2Command(myExecuteQuery, myConnection);
                myCommand.CommandTimeout = 9000000;
                myCommand.Connection.Open();
                myCommand.ExecuteNonQuery();
                myConnection.Close();
                //

            }
            catch (iDB2SQLErrorException e)
            {
                Console.WriteLine(e.Message);
            }
        }

    }
}
