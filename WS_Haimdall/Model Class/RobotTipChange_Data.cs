using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WS_Haimdall.Model_Class
{
    public class RobotTipChange_Data
    {
        public int ID { get; set; }

        public int? RobotID { get; set; }

        public int? GunID { get; set; }

        public string? Shift { get; set; }

        public int? Value { get; set; }

        public DateTime? Timstamp { get; set; }
    }
}
