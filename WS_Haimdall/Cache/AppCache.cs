using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WS_Haimdall.Cache
{
    public static class AppCache
    {
        /// <summary>
        /// Alaram static members
        /// </summary>
        public static Dictionary<int, string> dict_AlarmTags = new Dictionary<int, string>();

        /// <summary>
        /// NodeId Config static members
        /// </summary>
        public static ConcurrentDictionary<string, string> dict_NodeTest = new ConcurrentDictionary<string, string>();
        public static ConcurrentDictionary<string, string> dict_NodeIdConfigLineCT = new ConcurrentDictionary<string, string>();
        public static ConcurrentDictionary<string, string> dict_NodeIdConfigSubstationCT = new ConcurrentDictionary<string, string>();
        public static ConcurrentDictionary<string, string> dict_NodeIdConfigLineWiseProdData = new ConcurrentDictionary<string, string>();
        public static ConcurrentDictionary<string, string> dict_NodeIdConfigLosses = new ConcurrentDictionary<string, string>();
        public static ConcurrentDictionary<string, string> dict_NodeIdConfigOee = new ConcurrentDictionary<string, string>();
        public static ConcurrentDictionary<string, string> dict_NodeIdConfigMTTRMTBF = new ConcurrentDictionary<string, string>();
        public static ConcurrentDictionary<string, string> dict_NodeIdConfigRobotSpot = new ConcurrentDictionary<string, string>();
        public static ConcurrentDictionary<string, string> dict_NodeIdConfigRobotTipChange = new ConcurrentDictionary<string, string>();
        public static ConcurrentDictionary<string, string> dict_NodeIdConfigRobotStatus = new ConcurrentDictionary<string, string>();


        public static ConcurrentDictionary<string, string> dict_NodeIdConfigLineBuffer = new ConcurrentDictionary<string, string>();
        public static ConcurrentDictionary<string, string> dict_NodeIdConfigMarriageMismatch = new ConcurrentDictionary<string, string>();
    }
}
