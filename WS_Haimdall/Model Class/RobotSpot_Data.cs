using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WS_Haimdall.Model_Class
{
    public class RobotSpot_Data
    {
        public int ID { get; set; }

        public int? RobotID { get; set; }

        public int? GunID { get; set; }

        public string? Biwno { get; set; }

        public int? Varraint { get; set; }

        public int? SubVarraint { get; set; }

        public int? Target { get; set; }

        public int? Actual { get; set; }

        public DateTime? Timestamp { get; set; }

    }
}
