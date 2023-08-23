package alluxio.wire;

import alluxio.util.FormatUtils;
import com.amazonaws.util.StringUtils;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class FileSizeSerializer extends JsonSerializer<Long> {
    @Override
    public void serialize(Long value, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {
        String formattedSize = FormatUtils.getSizeFromBytes(value);
        jsonGenerator.writeString(StringUtils.upperCase(formattedSize));
    }
}
