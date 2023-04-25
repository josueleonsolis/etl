using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace ETLCountryPack
{
    public class Taxs
    {
        //VRSERIE
        [XmlElement("SITE")]
        public string SiteRef { get; set; }
        //VRFOLIO
        [XmlElement("BASE")]
        public string Base { get; set; }
        //VRDSER
        [XmlElement("TX")]
        public string Tax { get; set; }
        //VRDFOL
        [XmlElement("TF")]
        public string TypeFactor { get; set; }
        //VRDCUR
        [XmlElement("TC")]
        public string TasaOquote { get; set; }
        //VREXRT
        [XmlElement("TOT")]
        public string Total { get; set; }
        //VRMTPG
        [XmlElement("ID")]
        public string TaxConceptMultisiteID { get; set; }
        //VRNCUO
        [XmlElement("UUID")]
        public string UUID_DocRelated { get; set; }
        //VRPSDO
        [XmlElement("TT")]
        public string Type_Tax { get; set; }
        //VRMPAG
        [XmlElement("UFT1")]
        public string UFT1 { get; set; }
        //VRNSDO
        [XmlElement("UFD1")]
        public string UFD1 { get; set; }
        //VRDCID
        [XmlElement("UFAM")]
        public string UFAMT1 { get; set; }
        //SETSITE
        [XmlElement("SER")]
        public string Serie { get; set; }

        [XmlElement("FOL")]
        public string Folio { get; set; }

        [XmlElement("TOTF")]
        public string TotalInvoice { get; set; }

        [XmlElement("CUR")]
        public string CurrencyInvoice { get; set; }
    }


    public class CFDIDocRelated
    {
        [XmlElement("VJCONO")]
        public string VJCONO { get; set; }

        [XmlElement("VJSERIE")]
        public string VJSERIE { get; set; }

        [XmlElement("VJFOLIO")]
        public string VJFOLIO { get; set; }

        [XmlElement("VJSEQN")]
        public string VJSEQN { get; set; }

        [XmlElement("VJISERIE")]
        public string VJISERIE { get; set; }

        [XmlElement("VJIFOLIO")]
        public string VJIFOLIO { get; set; }

        [XmlElement("VJRELTYPE")]
        public string VJRELTYPE { get; set; }   
    }
}
