using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace ETLCountryPack
{
    public class MXEIPX
    {
        //VRSERIE
        [XmlElement("VRSERIE")]
        public string SeriePago { get; set; }
        //VRFOLIO
        [XmlElement("VRFOLIO")]
        public string FolioPago { get; set; }
        //VRDSER
        [XmlElement("VRDSER")]
        public string SerieDocRel { get; set; }
        //VRDFOL
        [XmlElement("VRDFOL")]
        public string FolioDocRel { get; set; }
        //VRDCUR
        [XmlElement("VRDCUR")]
        public string Moneda { get; set; }
        //VREXRT
        [XmlElement("VREXRT")]
        public string TipoCambio { get; set; }
        //VRMTPG
        [XmlElement("VRMTPG")]
        public string MetodoPago { get; set; }
        //VRNCUO
        [XmlElement("VRNCUO")]
        public string NumParcialidad { get; set; }
        //VRPSDO
        [XmlElement("VRPSDO")]
        public string SaldoAnterior { get; set; }
        //VRMPAG
        [XmlElement("VRMPAG")]
        public string MontoPagado { get; set; }
        //VRNSDO
        [XmlElement("VRNSDO")]
        public string NuevoSaldo { get; set; }
        //VRDCID
        [XmlElement("VRDCID")]
        public string UUID { get; set; }
        //SETSITE
        [XmlElement("SETSITE")]
        public string SETSITE { get; set; }
    }
}
