using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WS_Haimdall.Model_Class
{
    public class LineWiseProdData
    {
        public int? LineID { get; set; }
        public int? HourID { get; set; }
        public int? Target { get; set; }
        public int? Actual { get; set; }

        public int? J5_Target { get; set; }
        public int? J5_Actual { get; set; }

        public int? V23_Target { get; set; }
        public int? V23_Actual { get; set; }
        public DateTime? Timestamp { get; set; }
        public DateTime? LogDateTime { get; set; }
    }
}
