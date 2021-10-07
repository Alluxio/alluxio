package alluxio.stress;

import com.beust.jcommander.Parameter;

public abstract class GeneralParameters extends Parameters{
    public abstract Enum<?> operation();
}
