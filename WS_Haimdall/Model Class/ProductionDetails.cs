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

    public static class AlarmTags
    {
        public static Dictionary<int, string> dict_AlarmTags = new Dictionary<int, string>();
    }

    public static class NodeIdConfig
    {
        public static ConcurrentDictionary<string, string> dict_NodeIdConfigLineCT = new ConcurrentDictionary<string, string>();
        public static ConcurrentDictionary<string, string> dict_NodeIdConfigSubstationCT = new ConcurrentDictionary<string, string>();

    }
}
