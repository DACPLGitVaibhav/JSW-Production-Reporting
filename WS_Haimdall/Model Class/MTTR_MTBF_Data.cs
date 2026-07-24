using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WS_Haimdall.Model_Class
{
    public class MTTR_MTBF_Data
    {
        public int? ID { get; set; }
        public int? SubStationID { get; set; }
        public string? Shift { get; set; }

        public float? MTTR { get; set; }
        public float? MTBF { get; set; }

        public int? NoOfFailure { get; set; }
        public int? NetAvail_OperTime { get; set; }
        public int? BreakDownTime { get; set; }

        public DateTime? TimeStamp { get; set; }
    }
}
