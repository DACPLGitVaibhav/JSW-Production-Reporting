using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WS_Haimdall
{
   public class ProductionDetails
   {
        public int LineID { get; set; }
        public int Status { get; set; }
        public int ErpSeqNo { get; set; }
        public string ItemID { get; set; }
        public string BiwNo { get; set; }
        public string VCode { get; set; }
        public int Mes_Vcode { get; set; }
        public string ModelCode { get; set; }
        public string LOT { get; set; }
        public string LineName { get; set; }
   }

   
}
