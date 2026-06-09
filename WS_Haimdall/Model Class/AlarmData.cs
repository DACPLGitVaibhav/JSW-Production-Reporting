using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WS_Haimdall.Model_Class
{
    public class AlarmData
    {
        public string AlarmCode { get; set; }
        public object Value { get; set; }
        public string Action { get; set; }
        public DateTime Timestamp { get; set; }
    }
}
