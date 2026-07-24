using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WS_Haimdall.Model_Class
{
    public class LineBuffer
    {
        public int Id { get; set; }

        public int? LineID { get; set; }

        public int? Buffer_Count { get; set; }

        public int? Min_Threshold { get; set; }

        public int? Max_Threshold { get; set; }

        public int? Target { get; set; }

        public int? Status { get; set; }

        public DateTime? TimeStamp { get; set; }
    }
}
