package numbers;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Objects;

@XmlRootElement
public class Message {
    public long time;
    public String name;
    public String[] content;
    public String type;
    @XmlElement(name="long", nillable=true)
    public int longitude;
    public int lat;
    public Integer number;
    public int numbers[];

    public boolean equals(Object o) {
        Message message = (Message)o;

        return
            Objects.deepEquals(message.content, this.content) &&
            Objects.deepEquals(message.numbers, this.numbers) &&
            Objects.equals(message.time, this.time) &&
            Objects.equals(message.name, this.name) &&
            Objects.equals(message.type, this.type) &&
            Objects.equals(message.longitude, this.longitude) &&
            Objects.equals(message.lat, this.lat) &&
            Objects.equals(message.number, this.number);
    }
}
