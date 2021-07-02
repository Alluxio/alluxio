module Jekyll
  module Tags
    class AccordionTag < Liquid::Block
      def initialize(tag_name, block_options, liquid_options)
        super
        @accordionID = "accordion-#{block_options.strip}"
      end

      def render(context)
        context.stack do
          context["accordionID"] = @accordionID
          context["collapsed_idx"] = 1
          @content = super
        end
        output = %(<div class="accordion" id="#{@accordionID}">#{@content}</div>)

        output
      end
    end
  end
end

# taken from http://mikelui.io/2018/07/22/jekyll-nested-blocks.html
Liquid::Template.register_tag('accordion', Jekyll::Tags::AccordionTag)
