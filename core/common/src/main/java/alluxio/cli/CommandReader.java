package alluxio.cli;

public class CommandReader {
    private String Name = null;
    private  String Usage = null;
    private String Description = null;
    private String Example = null;
    private  String SubCommands = null;
    private String Options = null;

    public void setName(String name) {
        this.Name = name;
    }
    public String getName() {
        return this.Name;
    }

    public void setUsage(String Usage) {
        this.Usage = Usage;
    }
    public String getUsage() {
        return this.Usage;
    }

    public void setDescription(String Description) {
        this.Description = Description;
    }
    public String getDescription() {
        return this.Description;
    }

    public void setExample(String Example) {
        this.Example = Example;
    }
    public  String getExample(){
        return this.Example;
    }

    public void setSubCommands(String subCommands) {
        this.SubCommands = subCommands;
    }
    public String getSubCommands(){
        return this.SubCommands;
    }

    public void setOptions(String options) {
        this.Options = options;
    }
    public String getOptions(){
        return this.Options;
    }

    @Override
    public String toString(){
        return "name: "+ getName() +
                "\nusage: "+ getUsage() +
                "\ndescription: |\n  " + getDescription();
    }
}
