using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WS_Haimdall.Model_Class
{
    public class NodeConfg
    {
        public string Itemid_NodeId { get; set; }
        public string Biwno_NodeId { get; set; }
        public string Seqno_NodeId { get; set; }
        public string Modelcode_NodeId { get; set; }
        public string Data_Received_NodeId { get; set; }
        public string LOT_Seqno { get; set; }
        public string LOS_Seqno { get; set; }
        public string LOP_Seqno { get; set; }
        public string PRG_Empty { get; set; }
        public string LineName { get; set; }
        public int LineID { get; set; }
        public string OmsLineStatus { get; set; }
    }
}
