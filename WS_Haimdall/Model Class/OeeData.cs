using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WS_Haimdall.Model_Class
{
   
    public class OeeData
    {
        public int? ID { get; set; }
        public int? SubStationID { get; set; }
        public string? Shift { get; set; }

        public float? Availability { get; set; }
        public float? Performance { get; set; }
        public float? Quality { get; set; }
        public float? OEE { get; set; }

        public int? NetAvail_OperTime { get; set; }
        public int? BreakDownTime { get; set; }
        public int? PerformanceLossTime { get; set; }

        public DateTime? TimeStamp { get; set; }
    }
}
