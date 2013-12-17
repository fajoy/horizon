from horizon import forms
from ..hadoop.api import hadoop

class ShowForm(forms.SelfHandlingForm):
    class Meta:
        pass
    def __init__(self, *args, **kwargs):
        super(ShowForm, self).__init__(*args, **kwargs)
    def handle(self, request, data):
        return data

class ShowView(forms.ModalFormView):
    form_class = ShowForm
    template_name = 'custom/hadoop/show.html'

    def get_success_url(self):
        return reverse(self.success_url)

    def get_initial(self):
        initial = super(ShowView, self).get_initial()
        return initial

    def get_context_data(self, **kwargs):
        context = super(ShowView, self).get_context_data(**kwargs)
        context["title"] = "Show Context"
        context["name"] = self.kwargs["path"]
        text = ""
        try :
            text = hadoop.get_hadoop_obj(self.request,self.kwargs["path"])
        except:
            context["name"] += "   (not exist)"
            pass
        context["text"] = text
        return context
