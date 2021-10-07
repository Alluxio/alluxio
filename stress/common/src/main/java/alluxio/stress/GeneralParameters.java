package alluxio.stress;

import alluxio.stress.Operation;
import alluxio.stress.Parameters;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import java.util.EnumSet;

public abstract class GeneralParameters<T extends Enum<T>> extends Parameters {
    @Parameter(names = {"--operation"}, description =
        "the operation to perform for the stress bench", required = true)
    public T mOperation;
}
