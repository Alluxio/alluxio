package alluxio.stress.common;


import alluxio.stress.Parameters;

public abstract class GeneralParameters extends Parameters {
    public abstract Enum<?> operation();
}
