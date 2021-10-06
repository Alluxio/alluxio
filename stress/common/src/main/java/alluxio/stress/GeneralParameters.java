package alluxio.stress;

import alluxio.stress.Operation;
import alluxio.stress.Parameters;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import java.util.EnumSet;

public abstract class GeneralParameters<T extends Operation> extends Parameters {
    @Parameter(names = {"--operation"}, description =
        "the operation to perform for the stress bench", converter = OperationConverter.class, required = true)
    public T mOperation;

    public static class OperationConverter<S extends Operation> implements IStringConverter<S> {
        private final Class<S> clazz;

        protected OperationConverter(Class<S> clazz) {
            this.clazz = clazz;
        }

        @Override
        public S convert(String value) {

            return S.fromString(value, Class<T>);
        }
    }
    public class EnumConverter<T extends Enum<T>> implements IStringConverter<T> {

        private final String optionName;
        private final Class<T> clazz;

        /**
         * Constructs a new converter.
         * @param optionName the option name for error reporting
         * @param clazz the enum class
         */
        public EnumConverter(String optionName, Class<T> clazz) {
            this.optionName = optionName;
            this.clazz = clazz;
        }

        @Override
        public T convert(String value) {
            try {
                try {
                    return Enum.valueOf(clazz, value);
                } catch (IllegalArgumentException e) {
                    return Enum.valueOf(clazz, value.toUpperCase());
                }
            } catch (Exception e) {
                throw new ParameterException("Invalid value for " + optionName + " parameter. Allowed values:" +
                    EnumSet.allOf(clazz));

            }
        }
    }

}
