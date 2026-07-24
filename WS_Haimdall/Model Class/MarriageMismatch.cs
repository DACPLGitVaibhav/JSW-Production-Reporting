using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WS_Haimdall.Model_Class
{
    public class MarriageMismatch
    {
        public int Id { get; set; }

        public int? SubstaionID { get; set; }

        public int? Body1_Biw { get; set; }

        public int? Body1_Varraint { get; set; }

        public int? Body2_Biw { get; set; }

        public int? Body2_Varraint { get; set; }

        public int? Status { get; set; }

        public DateTime? TimeStamp { get; set; }
    }
}
