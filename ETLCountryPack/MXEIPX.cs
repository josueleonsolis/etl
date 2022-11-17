using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace ETLCountryPack
{
    public class PX
    {
        //VRSERIE
        [XmlElement("SER")]
        public string SeriePago { get; set; }
        //VRFOLIO
        [XmlElement("FOL")]
        public string FolioPago { get; set; }
        //VRDSER
        [XmlElement("DSER")]
        public string SerieDocRel { get; set; }
        //VRDFOL
        [XmlElement("DFOL")]
        public string FolioDocRel { get; set; }
        //VRDCUR
        [XmlElement("CUR")]
        public string Moneda { get; set; }
        //VREXRT
        [XmlElement("EXRT")]
        public string TipoCambio { get; set; }
        //VRMTPG
        [XmlElement("MTPG")]
        public string MetodoPago { get; set; }
        //VRNCUO
        [XmlElement("NCUO")]
        public string NumParcialidad { get; set; }
        //VRPSDO
        [XmlElement("PSDO")]
        public string SaldoAnterior { get; set; }
        //VRMPAG
        [XmlElement("MPAG")]
        public string MontoPagado { get; set; }
        //VRNSDO
        [XmlElement("NSDO")]
        public string NuevoSaldo { get; set; }
        //VRDCID
        [XmlElement("ID")]
        public string UUID { get; set; }
        //SETSITE
        [XmlElement("SETSITE")]
        public string SETSITE { get; set; }
    }
}
