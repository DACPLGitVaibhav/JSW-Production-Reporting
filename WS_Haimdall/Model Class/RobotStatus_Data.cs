using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WS_Haimdall.Model_Class
{
    public class RobotStatus_Data
    {
        public int ID { get; set; }

        public int? RobotID { get; set; }

        public int? HealthStatus { get; set; }

        public bool? BatteryStatus { get; set; }

        public DateTime? Timestamp { get; set; }
    }
}
