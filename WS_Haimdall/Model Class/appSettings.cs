using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WS_Haimdall.Model_Class
{
    //public class appSettings
    //{
    //    public string Endpoint { get; set; }
    //    public string IP { get; set; }
    //    public string LogPath { get; set; }        
    //    public string DB_Connection {  get; set; }

    //    public int PlcNo { get; set; }
    //    public bool isLastPlc { get; set; }

    //    public string Username { get; set; }
    //    public string Password { get; set; }
    //}

    public class appSettings
    { 
        public string Endpoint { get; set; } = string.Empty;
        public string IP { get; set; } = string.Empty;
        public string LogPath { get; set; } = string.Empty;
        public string DB_Connection { get; set; } = string.Empty;
        public int PlcNo { get; set; } = 0; 
        public bool isLastPlc { get; set; } = false; 
        public string Username { get; set; } = string.Empty;
        public string Password { get; set; } = string.Empty; }

}
