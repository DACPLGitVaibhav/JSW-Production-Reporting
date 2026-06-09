using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WS_Haimdall.Model_Class
{
    public class CycleTimeLineCTData
    {
        public int? Id { get; set; }
        public int? LineID { get; set; }
        public DateTime? StartTime { get; set; }
        public DateTime? EndTime { get; set; }
        public int? CycleTime { get; set; }
        public string? Biwno { get; set; }
        public int? VarriantCode { get; set; }
        public int? SubVarraintcode { get; set; }
        public DateTime? TimeStamp { get; set; }
    }
}
