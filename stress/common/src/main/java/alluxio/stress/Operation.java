package alluxio.stress;

import alluxio.stress.jobservice.JobServiceBenchOperation;

public interface Operation<T extends Enum<?>>{
    /**
     * Creates an instance type from the string. This method is case insensitive.
     *
     * @param text the instance type in string
     * @return the created instance
     */
    static <T> T fromString(String text, Class<T> enumType) {
        for (T type : enumType.getEnumConstants()) {
            if (type.toString().equalsIgnoreCase(text)) {
                return type;
            }
        }
        throw new IllegalArgumentException("No constant with text " + text + " found");
    }

    public T[] enumValues();

    @Override String toString();


}
